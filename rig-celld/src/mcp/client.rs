use std::{
    collections::BTreeSet,
    io::Write,
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
};

use rmcp::model::{Implementation, ListToolsResult};
use serde_json::{Map, Value, json};

use super::{Error, Limits, McpTool, PROTOCOL_VERSION, Request, Transport};

/// A stateless tools client. Clones share request-ID allocation, not sessions.
pub struct Client<T> {
    transport: Arc<T>,
    identity: Implementation,
    next_id: Arc<AtomicU32>,
    pub(crate) limits: Limits,
}

impl<T> Clone for Client<T> {
    fn clone(&self) -> Self {
        Self {
            transport: self.transport.clone(),
            identity: self.identity.clone(),
            next_id: self.next_id.clone(),
            limits: self.limits,
        }
    }
}

impl<T: Transport> Client<T> {
    pub fn new(transport: T, identity: Implementation) -> Self {
        Self {
            transport: Arc::new(transport),
            identity,
            next_id: Arc::new(AtomicU32::new(0)),
            limits: Limits::default(),
        }
    }

    pub fn with_limits(mut self, limits: Limits) -> Result<Self, Error> {
        if limits.request_bytes == 0
            || limits.response_bytes == 0
            || limits.catalog_bytes == 0
            || limits.max_pages == 0
            || limits.max_tools == 0
            || limits.timeout.is_zero()
            || limits.timeout.as_millis() > i32::MAX as u128
        {
            return Err(Error::Configuration(
                "limits must be positive and timeout must fit i32 milliseconds",
            ));
        }
        self.limits = limits;
        Ok(self)
    }

    /// Discover all pages under fixed byte, page, and tool limits. Tools with
    /// invalid header annotations are excluded as required by the HTTP binding.
    /// This does not cache discovery or infer authority from tool annotations.
    pub async fn list_tools(&self) -> Result<Vec<McpTool<T>>, Error> {
        let mut tools = Vec::new();
        let mut names = BTreeSet::new();
        let mut cursors = BTreeSet::new();
        let mut cursor = None;
        let mut remaining = self.limits.catalog_bytes;
        let mut count = 0usize;
        for _ in 0..self.limits.max_pages {
            let mut params = Map::new();
            if let Some(cursor) = cursor {
                params.insert("cursor".into(), Value::String(cursor));
            }
            let (result, bytes) = self
                .rpc("tools/list", params, Vec::new(), remaining)
                .await?;
            remaining -= bytes;
            let page: ListToolsResult = serde_json::from_value(result)
                .map_err(|_| Error::Protocol("invalid tools/list result"))?;
            count = count
                .checked_add(page.tools.len())
                .ok_or(Error::Limit("tool count"))?;
            if count > self.limits.max_tools {
                return Err(Error::Limit("tool count"));
            }
            for definition in page.tools {
                if !names.insert(definition.name.to_string()) {
                    return Err(Error::Protocol("duplicate tool name"));
                }
                match self.tool(definition) {
                    Ok(tool) => tools.push(tool),
                    Err(Error::HeaderAnnotation) => {
                        tracing::warn!("excluded MCP tool with invalid header annotations")
                    }
                    Err(error) => return Err(error),
                }
            }
            cursor = page.next_cursor;
            match &cursor {
                None => return Ok(tools),
                Some(cursor) if !cursors.insert(cursor.clone()) => {
                    return Err(Error::Protocol("repeated pagination cursor"));
                }
                Some(_) => {}
            }
        }
        Err(Error::Limit("discovery pages"))
    }

    /// Bind a retained, host-approved definition without repeating discovery.
    /// Persist and version definitions with durable histories before effects.
    pub fn tool(&self, definition: rmcp::model::Tool) -> Result<McpTool<T>, Error> {
        McpTool::new(self.clone(), definition)
    }

    pub(crate) async fn rpc(
        &self,
        method: &'static str,
        mut params: Map<String, Value>,
        mut headers: Vec<(String, String)>,
        max_response: usize,
    ) -> Result<(Value, usize), Error> {
        let id = self
            .next_id
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
            .map_err(|_| Error::Limit("request IDs"))?
            .to_string();
        params.insert(
            "_meta".into(),
            json!({
                "io.modelcontextprotocol/protocolVersion": PROTOCOL_VERSION,
                "io.modelcontextprotocol/clientInfo": self.identity,
                "io.modelcontextprotocol/clientCapabilities": {},
            }),
        );
        let body = encode(
            &json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params}),
            self.limits.request_bytes,
        )?;
        headers.extend([
            ("Content-Type".into(), "application/json".into()),
            ("Accept".into(), "application/json".into()),
            ("MCP-Protocol-Version".into(), PROTOCOL_VERSION.into()),
            ("Mcp-Method".into(), method.into()),
        ]);
        let limits = Limits {
            response_bytes: self.limits.response_bytes.min(max_response),
            ..self.limits
        };
        if limits.response_bytes == 0 {
            return Err(Error::Limit("response bytes"));
        }
        let response = self
            .transport
            .post(Request { headers, body }, limits)
            .await?;
        if response.body.len() > limits.response_bytes {
            return Err(Error::Limit("response bytes"));
        }
        let success = (200..300).contains(&response.status);
        let invalid = |message| {
            if success {
                Error::Protocol(message)
            } else {
                Error::Http(response.status)
            }
        };
        if !is_json(&response.content_type) {
            return Err(if success {
                Error::ContentType
            } else {
                Error::Http(response.status)
            });
        }
        let value: Value =
            serde_json::from_slice(&response.body).map_err(|_| invalid("invalid JSON"))?;
        if value.get("jsonrpc").and_then(Value::as_str) != Some("2.0")
            || value.get("id").and_then(Value::as_str) != Some(&id)
            || value.get("result").is_some() == value.get("error").is_some()
        {
            return Err(invalid("invalid envelope or mismatched request ID"));
        }
        if let Some(error) = value.get("error") {
            let code = error
                .get("code")
                .and_then(Value::as_i64)
                .ok_or_else(|| invalid("invalid error code"))?;
            if error.get("message").and_then(Value::as_str).is_none() {
                return Err(invalid("invalid error message"));
            }
            return Err(Error::Rpc(code));
        }
        if !success {
            return Err(Error::Http(response.status));
        }
        let result = value
            .get("result")
            .ok_or(Error::Protocol("missing result"))?;
        match result.get("resultType").and_then(Value::as_str) {
            Some("complete") => Ok((result.clone(), response.body.len())),
            Some("input_required") => Err(Error::Unsupported("input_required")),
            Some("task") => Err(Error::Unsupported("task")),
            _ => Err(Error::Protocol("expected a 2026-07-28 complete result")),
        }
    }
}

pub(crate) fn is_json(content_type: &str) -> bool {
    content_type
        .split(';')
        .next()
        .is_some_and(|mime| mime.trim().eq_ignore_ascii_case("application/json"))
}

pub(crate) fn encode(value: &impl serde::Serialize, max: usize) -> Result<Vec<u8>, Error> {
    struct Bounded {
        bytes: Vec<u8>,
        max: usize,
    }
    impl Write for Bounded {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if bytes.len() > self.max - self.bytes.len() {
                return Err(std::io::Error::other("byte limit"));
            }
            self.bytes.extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut writer = Bounded {
        bytes: Vec::new(),
        max,
    };
    serde_json::to_writer(&mut writer, value).map_err(|_| Error::Limit("encoded bytes"))?;
    Ok(writer.bytes)
}
