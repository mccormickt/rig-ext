use super::{HttpTransport, check_content_type};
use crate::mcp::{Error, Limits, Request, Response};
use futures_util::future::{Either, select};
use worker::{
    js_sys::{self, Function, Object, Promise, Reflect, Uint8Array},
    wasm_bindgen::{JsCast, JsValue},
    wasm_bindgen_futures::{JsFuture, spawn_local},
    web_sys,
};

impl HttpTransport {
    pub(super) async fn post_http(
        &self,
        request: Request,
        limits: Limits,
    ) -> Result<Response, Error> {
        // Delay uses a signed 32-bit millisecond timer in the Worker runtime.
        if limits.timeout.as_millis() > i32::MAX as u128 {
            return Err(Error::Configuration("HTTP timeout is too large"));
        }
        let abort = Abort(web_sys::AbortController::new().map_err(|_| Error::Transport)?);
        let operation = async {
            let options = Object::new();
            set(&options, "method", &"POST".into())?;
            set(&options, "redirect", &"manual".into())?;
            set(&options, "signal", abort.0.signal().as_ref())?;
            set(&options, "body", &Uint8Array::from(request.body.as_slice()))?;
            let headers = web_sys::Headers::new().map_err(|_| Error::Transport)?;
            for (name, value) in request.headers {
                headers
                    .append(&name, &value)
                    .map_err(|_| Error::Configuration("invalid HTTP header"))?;
            }
            if let Some(value) = &self.bearer {
                headers
                    .set("authorization", value)
                    .map_err(|_| Error::Configuration("invalid authorization value"))?;
            }
            set(&options, "headers", headers.as_ref())?;
            let global = js_sys::global();
            let result = method(global.as_ref(), "fetch")?
                .call2(&global, &self.endpoint.as_str().into(), &options)
                .map_err(|_| Error::Transport)?;
            let response: web_sys::Response = JsFuture::from(Promise::resolve(&result))
                .await
                .map_err(|_| Error::Transport)?
                .dyn_into()
                .map_err(|_| Error::Transport)?;
            let status = response.status();
            let mut reader = response
                .body()
                .map(|stream| {
                    let raw = stream.as_ref();
                    Ok::<_, Error>(BodyReader {
                        reader: method(raw, "getReader")?
                            .call0(raw)
                            .map_err(|_| Error::Transport)?,
                        done: false,
                    })
                })
                .transpose()?;
            let content_type = response
                .headers()
                .get("content-type")
                .map_err(|_| Error::Transport)?
                .unwrap_or_default();
            check_content_type(&content_type, status)?;
            let mut body = Vec::new();
            if let Some(reader) = &mut reader {
                loop {
                    let next = method(&reader.reader, "read")?
                        .call0(&reader.reader)
                        .map_err(|_| Error::Transport)?;
                    let next = JsFuture::from(Promise::resolve(&next))
                        .await
                        .map_err(|_| Error::Transport)?;
                    if get(&next, "done")?.as_bool() == Some(true) {
                        reader.done = true;
                        break;
                    }
                    let chunk: Uint8Array = get(&next, "value")?
                        .dyn_into()
                        .map_err(|_| Error::Transport)?;
                    let length = chunk.length() as usize;
                    if length > limits.response_bytes - body.len() {
                        return Err(Error::Limit("response bytes"));
                    }
                    let start = body.len();
                    body.resize(start + length, 0);
                    chunk.copy_to(&mut body[start..]);
                }
            }
            Ok(Response {
                status,
                content_type,
                body,
            })
        };
        let deadline = worker::Delay::from(limits.timeout);
        futures_util::pin_mut!(operation, deadline);
        match select(deadline, operation).await {
            Either::Left(_) => Err(Error::Timeout),
            Either::Right((result, _)) => result,
        }
        // Both successful completion and every early exit/drop abort local I/O.
    }
}

struct Abort(web_sys::AbortController);
impl Drop for Abort {
    fn drop(&mut self) {
        self.0.abort();
    }
}

struct BodyReader {
    reader: JsValue,
    done: bool,
}
impl Drop for BodyReader {
    fn drop(&mut self) {
        if !self.done
            && let Ok(cancel) = method(&self.reader, "cancel")
            && let Ok(result) = cancel.call0(&self.reader)
        {
            spawn_local(async move {
                let _ = JsFuture::from(Promise::resolve(&result)).await;
            });
        }
        if let Ok(release) = method(&self.reader, "releaseLock") {
            let _ = release.call0(&self.reader);
        }
    }
}

fn get(target: &JsValue, name: &str) -> Result<JsValue, Error> {
    Reflect::get(target, &name.into()).map_err(|_| Error::Transport)
}
fn method(target: &JsValue, name: &str) -> Result<Function, Error> {
    get(target, name)?.dyn_into().map_err(|_| Error::Transport)
}
fn set(target: &Object, name: &str, value: &JsValue) -> Result<(), Error> {
    if Reflect::set(target, &name.into(), value).map_err(|_| Error::Transport)? {
        Ok(())
    } else {
        Err(Error::Transport)
    }
}
