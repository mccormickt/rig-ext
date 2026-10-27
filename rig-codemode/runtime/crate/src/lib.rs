use wasm_bindgen::prelude::*;

/// QuickJS has no observable clock in replay scripts.
#[unsafe(no_mangle)]
pub extern "C" fn __rquickjs_host_now_us() -> f64 {
    0.0
}

#[wasm_bindgen]
pub fn exchange(body: &str) -> Result<String, JsValue> {
    rig_codemode::worker_runtime::exchange(body, execute_job).map_err(|error| {
        JsValue::from_f64(match error {
            rig_codemode::replay::ReplayError::BudgetExceeded => 429.0,
            _ => 422.0,
        })
    })
}

fn execute_job(ctx: &rquickjs::Ctx<'_>) -> rquickjs::Result<bool> {
    // SAFETY: the exchange owns one context and holds its runtime lock.
    // QuickJS leaves a failed job's exception on that context.
    let result = unsafe {
        let runtime = rquickjs::qjs::JS_GetRuntime(ctx.as_raw().as_ptr());
        let mut context = std::ptr::null_mut();
        rquickjs::qjs::JS_ExecutePendingJob(runtime, &mut context)
    };
    if result < 0 || ctx.has_exception() {
        Err(rquickjs::Error::Exception)
    } else {
        Ok(result != 0)
    }
}
