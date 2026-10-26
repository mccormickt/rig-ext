//! Bounded, single-attempt JSON HTTP adapters.

use super::{Error, Limits, Request, Response, Transport};
use rig_core::wasm_compat::WasmBoxedFuture;

#[cfg(not(target_family = "wasm"))]
mod native;
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
mod workers;

/// Application-configured HTTP endpoint. Credentials are never formatted.
#[derive(Clone)]
pub struct HttpTransport {
    endpoint: url::Url,
    bearer: Option<String>,
    #[cfg(not(target_family = "wasm"))]
    client: reqwest::Client,
}

impl std::fmt::Debug for HttpTransport {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Even the endpoint path and query can contain application secrets.
        formatter
            .debug_struct("HttpTransport")
            .finish_non_exhaustive()
    }
}

impl HttpTransport {
    pub fn new(endpoint: &str) -> Result<Self, Error> {
        let url =
            url::Url::parse(endpoint).map_err(|_| Error::Configuration("invalid HTTP endpoint"))?;
        let authority = endpoint
            .split_once("://")
            .map(|(_, rest)| rest.split(['/', '?', '#']).next().unwrap_or_default());
        if !matches!(url.scheme(), "http" | "https")
            || url.host_str().is_none()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.fragment().is_some()
            || authority.is_none_or(|value| value.contains('@'))
        {
            return Err(Error::Configuration("invalid HTTP endpoint"));
        }
        Ok(Self {
            endpoint: url,
            bearer: None,
            #[cfg(not(target_family = "wasm"))]
            client: reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .retry(reqwest::retry::never())
                .build()
                .map_err(|_| Error::Transport)?,
        })
    }

    pub fn bearer_auth(mut self, token: impl Into<String>) -> Result<Self, Error> {
        let token = token.into();
        validate_value(&token)?;
        if token.is_empty() {
            return Err(Error::Configuration("invalid authorization value"));
        }
        self.bearer = Some(format!("Bearer {token}"));
        Ok(self)
    }
}

impl Transport for HttpTransport {
    fn post(
        &self,
        request: Request,
        limits: Limits,
    ) -> WasmBoxedFuture<'_, Result<Response, Error>> {
        Box::pin(async move {
            if request.body.len() > limits.request_bytes {
                return Err(Error::Limit("request bytes"));
            }
            if limits.timeout.is_zero() {
                return Err(Error::Timeout);
            }
            for (name, value) in &request.headers {
                if name.is_empty()
                    || !name
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&b))
                {
                    return Err(Error::Configuration("invalid HTTP header name"));
                }
                validate_value(value)?;
            }
            self.post_http(request, limits).await
        })
    }
}

fn validate_value(value: &str) -> Result<(), Error> {
    if value.trim() != value || !value.bytes().all(|b| (0x20..=0x7e).contains(&b)) {
        return Err(Error::Configuration("invalid HTTP header value"));
    }
    Ok(())
}

fn check_content_type(value: &str, status: u16) -> Result<(), Error> {
    if super::client::is_json(value) {
        Ok(())
    } else if (200..300).contains(&status) {
        Err(Error::ContentType)
    } else {
        Err(Error::Http(status))
    }
}
