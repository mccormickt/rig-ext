use worker::js_sys::{Reflect, Uint8Array};
use worker::wasm_bindgen::{JsCast, JsValue};
use worker::wasm_bindgen_futures::{JsFuture, spawn_local};
use worker::web_sys::ReadableStreamDefaultReader;
use worker::{Context, Env, Request, Response, event};

use crate::execute_job;

#[event(fetch)]
pub async fn fetch(request: Request, _: Env, _: Context) -> worker::Result<Response> {
    let result = match read_body(&request).await {
        Ok(body) => {
            rig_codemode::worker_runtime::exchange(&body, execute_job).map_err(
                |error| match error {
                    rig_codemode::replay::ReplayError::BudgetExceeded => 429,
                    _ => 422,
                },
            )
        }
        Err(status) => Err(status),
    };
    match result {
        Ok(body) => {
            let mut response = Response::ok(body)?;
            response
                .headers_mut()
                .set("Content-Type", "application/json")?;
            Ok(response)
        }
        Err(status) => Response::error("replay exchange failed", status),
    }
}

struct Reader(ReadableStreamDefaultReader);

impl Drop for Reader {
    fn drop(&mut self) {
        let cancelled = self.0.cancel();
        spawn_local(async move {
            let _ = JsFuture::from(cancelled).await;
        });
        self.0.release_lock();
    }
}

async fn read_body(request: &Request) -> Result<String, u16> {
    let stream = request.inner().body().ok_or(422_u16)?;
    let reader = Reader(stream.get_reader().unchecked_into());
    let mut bytes = Vec::new();
    loop {
        let chunk = JsFuture::from(reader.0.read()).await.map_err(|_| 422_u16)?;
        let done = Reflect::get(&chunk, &JsValue::from_str("done")).map_err(|_| 422_u16)?;
        if done.as_bool() == Some(true) {
            break;
        }
        let value = Reflect::get(&chunk, &JsValue::from_str("value"))
            .map_err(|_| 422_u16)?
            .dyn_into::<Uint8Array>()
            .map_err(|_| 422_u16)?;
        let len = value.length() as usize;
        if len > rig_codemode::worker_runtime::MAX_TRANSPORT - bytes.len() {
            return Err(429);
        }
        let start = bytes.len();
        bytes.resize(start + len, 0);
        value.copy_to(&mut bytes[start..]);
    }
    String::from_utf8(bytes).map_err(|_| 422)
}
