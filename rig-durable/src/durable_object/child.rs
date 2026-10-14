//! Namespace-addressed children. Routing and authorization belong to the application.

use std::rc::Rc;

use futures::future::LocalBoxFuture;
use serde::{Deserialize, Serialize};

use super::{Error, storage};
use crate::{DurableResponse, LogicalCallKey, SubmitInput, config::ConfigSnapshot};

/// A credential-free registration retained by the parent before submission.
/// The application must change `implementation_version` when model or tool
/// implementation changes are not represented in the configuration.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChildDefinition {
    pub implementation_version: String,
    pub config: ConfigSnapshot,
}

impl ChildDefinition {
    pub fn digest(&self) -> Result<String, Error> {
        Ok(crate::identity::arguments_digest(&serde_json::to_value(
            self,
        )?)?)
    }

    pub(super) fn validate(&self) -> Result<(), Error> {
        if self.implementation_version.is_empty() {
            return Err(Error::Invalid(
                "child implementation version is empty".into(),
            ));
        }
        if self.config.approval.enabled
            || self.config.tools.iter().any(|tool| tool.requires_approval)
        {
            return Err(Error::Invalid("nested approvals are not supported".into()));
        }
        Ok(())
    }
}

/// A stable namespace name derived from the complete outer call identity and
/// nested ordinal. Ordinary model child calls use `None` for the ordinal.
pub fn child_id(parent: &LogicalCallKey, ordinal: Option<u64>) -> String {
    use sha2::{Digest, Sha256};
    let key = format!("child-v1;{};{ordinal:?}", parent.canonical());
    format!("rig-child-{:x}", Sha256::digest(key.as_bytes()))
}

/// One app-routed endpoint accepts both admission and committed-state polling.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case")]
pub enum ChildRequest {
    Submit {
        child_id: String,
        expected: ChildDefinition,
        input: SubmitInput,
    },
    Poll {
        child_id: String,
        expected: ChildDefinition,
        request_id: String,
    },
}

impl ChildRequest {
    pub fn child_id(&self) -> &str {
        match self {
            Self::Submit { child_id, .. } | Self::Poll { child_id, .. } => child_id,
        }
    }

    pub(super) fn expected(&self) -> &ChildDefinition {
        match self {
            Self::Submit { expected, .. } | Self::Poll { expected, .. } => expected,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "status", content = "result", rename_all = "snake_case")]
pub enum ChildReply {
    Pending,
    Completed(Box<DurableResponse>),
    Failed(String),
    /// A permanent admission/configuration error, not an uncertain transport ack.
    Rejected(String),
}

/// Maximum serialized request or reply size for the child protocol.
pub const CHILD_PROTOCOL_MAX_BYTES: usize = storage::ROW_BYTES;

impl ChildReply {
    pub(super) fn decode(bytes: &[u8]) -> Result<Self, Error> {
        if bytes.len() > CHILD_PROTOCOL_MAX_BYTES {
            return Err(Error::Protocol("child reply exceeds the child protocol byte limit; its effect may still be running".into()));
        }
        serde_json::from_slice(bytes).map_err(|error| {
            Error::Protocol(format!("child endpoint returned an invalid protocol reply; its effect may still be running: {error}"))
        })
    }

    pub(super) fn completed(result: DurableResponse) -> Result<Self, Error> {
        let reply = Self::Completed(Box::new(result));
        if serde_json::to_vec(&reply)?.len() > CHILD_PROTOCOL_MAX_BYTES {
            return Ok(Self::Failed("committed child response exceeds the child protocol byte limit; the full result remains stored on the child".into()));
        }
        Ok(reply)
    }
}

/// An error means acknowledgement is uncertain. The parent stores a retry
/// deadline and sends the identical request again. Permanent failures must
/// return `ChildReply::Rejected` or `ChildReply::Failed`.
pub trait ChildTransport {
    fn request<'a>(
        &'a self,
        request: &'a ChildRequest,
    ) -> LocalBoxFuture<'a, Result<ChildReply, Error>>;
}

#[derive(Clone)]
pub struct ChildHandle {
    pub definition: ChildDefinition,
    pub(super) transport: Rc<dyn ChildTransport>,
}

impl ChildHandle {
    pub fn new(definition: ChildDefinition, transport: impl ChildTransport + 'static) -> Self {
        Self {
            definition,
            transport: Rc::new(transport),
        }
    }
}

/// Ordinary Durable Object namespace transport. No stub is retained in SQL.
/// `endpoint` selects the application's authenticated child protocol route.
pub struct NamespaceTransport {
    namespace: worker::ObjectNamespace,
    endpoint: String,
}

impl NamespaceTransport {
    pub fn new(namespace: worker::ObjectNamespace, endpoint: impl Into<String>) -> Self {
        Self {
            namespace,
            endpoint: endpoint.into(),
        }
    }
}

impl ChildTransport for NamespaceTransport {
    fn request<'a>(
        &'a self,
        request: &'a ChildRequest,
    ) -> LocalBoxFuture<'a, Result<ChildReply, Error>> {
        Box::pin(async move {
            let body = match storage::encode(request, CHILD_PROTOCOL_MAX_BYTES) {
                Ok(body) => body,
                Err(Error::Limit { .. }) => {
                    return Ok(ChildReply::Rejected(
                        "child request exceeds the child protocol byte limit".into(),
                    ));
                }
                result => result?,
            };
            let headers = worker::Headers::new();
            headers.set("content-type", "application/json")?;
            let mut init = worker::RequestInit::new();
            init.with_method(worker::Method::Post)
                .with_headers(headers)
                .with_body(Some(body.into()));
            let stub = self
                .namespace
                .id_from_name(request.child_id())?
                .get_stub()?;
            let mut response = stub
                .fetch_with_request(worker::Request::new_with_init(&self.endpoint, &init)?)
                .await?;
            if response.status_code() != 200 {
                return Err(Error::Storage(format!(
                    "child endpoint returned HTTP {}",
                    response.status_code()
                )));
            }
            use futures::StreamExt;
            let mut stream = response.stream()?;
            let mut bytes = Vec::new();
            while let Some(chunk) = stream.next().await {
                let chunk = chunk?;
                if bytes.len().saturating_add(chunk.len()) > CHILD_PROTOCOL_MAX_BYTES {
                    return Err(Error::Protocol("child reply exceeds the child protocol byte limit; its effect may still be running".into()));
                }
                bytes.extend_from_slice(&chunk);
            }
            ChildReply::decode(&bytes)
        })
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub(super) struct ChildInvocation {
    pub request: ChildRequest,
    pub acknowledged: bool,
}

impl ChildInvocation {
    pub fn new(
        parent: &LogicalCallKey,
        ordinal: Option<u64>,
        expected: ChildDefinition,
        message: String,
    ) -> Self {
        let id = child_id(parent, ordinal);
        Self {
            request: ChildRequest::Submit {
                child_id: id.clone(),
                expected,
                input: SubmitInput::new(id, message),
            },
            acknowledged: false,
        }
    }

    pub fn outbound(&self) -> ChildRequest {
        if self.acknowledged
            && let ChildRequest::Submit {
                child_id,
                expected,
                input,
            } = &self.request
        {
            return ChildRequest::Poll {
                child_id: child_id.clone(),
                expected: expected.clone(),
                request_id: input.request_id.clone(),
            };
        }
        self.request.clone()
    }
}
