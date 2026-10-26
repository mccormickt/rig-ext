use super::{HttpTransport, check_content_type};
use crate::mcp::{Error, Limits, Request, Response};

impl HttpTransport {
    pub(super) async fn post_http(
        &self,
        request: Request,
        limits: Limits,
    ) -> Result<Response, Error> {
        // reqwest's total request timeout stays attached to the response body.
        let mut builder = self
            .client
            .post(self.endpoint.clone())
            .timeout(limits.timeout);
        for (name, value) in request.headers {
            builder = builder.header(name, value);
        }
        if let Some(value) = &self.bearer {
            builder = builder.header(reqwest::header::AUTHORIZATION, value);
        }
        let mut response = builder.body(request.body).send().await.map_err(map_error)?;
        let status = response.status().as_u16();
        let content_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_owned();
        check_content_type(&content_type, status)?;
        if response
            .content_length()
            .is_some_and(|length| length > limits.response_bytes as u64)
        {
            return Err(Error::Limit("response bytes"));
        }
        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(map_error)? {
            if chunk.len() > limits.response_bytes - body.len() {
                return Err(Error::Limit("response bytes"));
            }
            body.extend_from_slice(&chunk);
        }
        Ok(Response {
            status,
            content_type,
            body,
        })
    }
}

fn map_error(error: reqwest::Error) -> Error {
    if error.is_timeout() {
        Error::Timeout
    } else {
        Error::Transport
    }
}
