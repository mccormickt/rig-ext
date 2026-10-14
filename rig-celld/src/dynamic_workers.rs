//! Fresh dynamic Workers through the runtime's Worker Loader binding.

use std::collections::BTreeMap;

use worker::{
    Env, Error, Fetcher, Headers, Request, Result,
    js_sys::{Array, Function, Object, Promise, Reflect, Uint8Array},
    wasm_bindgen::{JsCast, JsValue},
    wasm_bindgen_futures::{JsFuture, spawn_local},
    web_sys,
};

/// A JavaScript ES module or a compiled WASM module supplied to the Loader.
#[derive(Debug)]
pub enum WorkerModule {
    JavaScript(String),
    Wasm(Vec<u8>),
}

/// Explicit network policy for the dynamic Worker's global fetch/connect.
#[derive(Debug)]
pub enum OutboundPolicy {
    /// Block global outbound access.
    Deny,
    /// Inherit the parent Worker's outbound access.
    Inherit,
    /// Route outbound access through a service binding.
    /// celld 0.6.2 supports fetch through this policy, not connect or WebSockets.
    Service(Fetcher),
}

/// Runtime per-invocation limits, not wall-time deadlines.
///
/// celld 0.6.2 enforces these limits. Local workerd 1.20260730.1 does not
/// enforce either limit. Cloudflare production enforcement is not established
/// by local tests. The enclosing runtime may impose lower limits.
/// Where enforced, a subrequest limit of zero denies all subrequests.
#[derive(Clone, Copy, Debug)]
pub struct WorkerLimits {
    pub cpu_ms: u32,
    pub subrequests: u32,
}

/// Code and policy for a fresh Worker. Its environment is always empty.
#[derive(Debug)]
pub struct WorkerCode {
    pub main_module: String,
    pub modules: BTreeMap<String, WorkerModule>,
    pub compatibility_date: String,
    pub compatibility_flags: Vec<String>,
    pub outbound: OutboundPolicy,
    pub limits: WorkerLimits,
}

/// A Worker Loader binding, independent of Durable Object storage.
#[derive(Clone, Debug)]
pub struct WorkerLoader {
    binding: JsValue,
}

impl WorkerLoader {
    /// Read a Loader binding from the application environment.
    pub fn from_env(env: &Env, binding_name: &str) -> Result<Self> {
        let binding = Reflect::get(env.as_ref(), &JsValue::from_str(binding_name))?;
        method(&binding, "load")?;
        Ok(Self { binding })
    }

    /// Load a fresh anonymous Worker, without an ID cache or facets.
    ///
    /// Runtime compilation failures can surface on the first fetch, since the
    /// runtime can load code lazily behind its synchronous WorkerStub.
    pub fn load(&self, code: WorkerCode) -> Result<LoadedWorker> {
        let config = Object::new();
        set(&config, "mainModule", &code.main_module.into())?;
        set(
            &config,
            "compatibilityDate",
            &code.compatibility_date.into(),
        )?;
        let flags: Array = code
            .compatibility_flags
            .into_iter()
            .map(JsValue::from)
            .collect();
        set(&config, "compatibilityFlags", &flags)?;
        let modules = Object::new();
        for (name, module) in code.modules {
            let value = Object::new();
            match module {
                WorkerModule::JavaScript(source) => set(&value, "js", &source.into())?,
                WorkerModule::Wasm(bytes) => {
                    set(&value, "wasm", &Uint8Array::from(bytes.as_slice()).buffer())?;
                }
            }
            // Define an own property, including module names such as __proto__.
            let descriptor = Object::new();
            set(&descriptor, "value", &value)?;
            set(&descriptor, "enumerable", &JsValue::TRUE)?;
            Reflect::define_property(&modules, &name.into(), &descriptor)?;
        }
        set(&config, "modules", &modules)?;
        set(&config, "env", &Object::new())?;
        match code.outbound {
            OutboundPolicy::Deny => set(&config, "globalOutbound", &JsValue::NULL)?,
            OutboundPolicy::Inherit => {}
            OutboundPolicy::Service(fetcher) => set(&config, "globalOutbound", fetcher.as_ref())?,
        }
        let limits = Object::new();
        set(&limits, "cpuMs", &code.limits.cpu_ms.into())?;
        set(&limits, "subRequests", &code.limits.subrequests.into())?;
        set(&config, "limits", &limits)?;
        let stub = method(&self.binding, "load")?.call1(&self.binding, &config)?;
        Ok(LoadedWorker { stub: Some(stub) })
    }
}

/// Owns a loaded Worker until explicit cleanup or drop.
///
/// On celld 0.6.2 cleanup requests asynchronous release; it does not cancel
/// in-flight work or acknowledge shutdown. On workerd 1.20260730.1, the stub
/// has no disposer and cleanup drops references for runtime GC.
#[derive(Debug)]
pub struct LoadedWorker {
    stub: Option<JsValue>,
}

impl LoadedWorker {
    /// Access the default (`None`) or a named entrypoint.
    ///
    /// Keep this owner alive through every use of the returned Fetcher and its
    /// response bodies. Do not dispose the entrypoint: an unknown method name
    /// on celld can become an RPC call instead of local cleanup.
    pub fn entrypoint(&self, name: Option<&str>) -> Result<Fetcher> {
        let stub = self
            .stub
            .as_ref()
            .ok_or_else(|| Error::RustError("dynamic Worker has been disposed".to_owned()))?;
        let name = name.map(JsValue::from_str).unwrap_or(JsValue::NULL);
        let entrypoint = method(stub, "getEntrypoint")?.call1(stub, &name)?;
        // Fetcher has no JS constructor usable for an instanceof check.
        method(&entrypoint, "fetch")?;
        Ok(entrypoint.unchecked_into())
    }

    /// Fetch and consume at most `max_bytes` response bytes.
    ///
    /// The owner and Fetcher remain live through body consumption. Each JS
    /// chunk's length is checked before copying into Rust. The bound applies
    /// to the collected body, not the runtime's JS buffers or elapsed time.
    /// Callers must separately bound their request and provide a deadline.
    /// On a body error or overflow, request stream cancellation without
    /// replacing the primary error or waiting for cancellation to complete.
    pub async fn fetch_bounded(
        &self,
        name: Option<&str>,
        request: Request,
        max_bytes: usize,
    ) -> Result<BoundedResponse> {
        let fetcher = self.entrypoint(name)?;
        // Use the raw response so the bound is checked before ByteStream's
        // per-chunk Vec allocation, and worker's optional http feature works.
        let response =
            method(fetcher.as_ref(), "fetch")?.call1(fetcher.as_ref(), request.inner())?;
        let response: web_sys::Response = JsFuture::from(Promise::resolve(&response))
            .await?
            .dyn_into()?;
        let status = response.status();
        let headers = worker::Response::from(Clone::clone(&response))
            .headers()
            .clone();
        let mut body = Vec::new();
        if let Some(stream) = response.body() {
            let raw: &JsValue = stream.as_ref();
            let reader = method(raw, "getReader")?.call0(raw)?;
            let mut reader = BodyReader {
                reader,
                done: false,
            };
            loop {
                let next = method(&reader.reader, "read")?.call0(&reader.reader)?;
                let next = JsFuture::from(Promise::resolve(&next)).await?;
                if Reflect::get(&next, &"done".into())?.as_bool() == Some(true) {
                    reader.done = true;
                    break;
                }
                let chunk: Uint8Array = Reflect::get(&next, &"value".into())?.dyn_into()?;
                let length = chunk.length() as usize;
                if length > max_bytes - body.len() {
                    return Err(Error::RustError(format!(
                        "dynamic Worker response exceeds {max_bytes} bytes"
                    )));
                }
                let start = body.len();
                body.resize(start + length, 0);
                chunk.copy_to(&mut body[start..]);
            }
        }
        Ok(BoundedResponse {
            status,
            headers,
            body,
        })
    }

    /// Drop owned references, once, and request release if a disposer exists.
    ///
    /// Neither the return nor drop acknowledges release or cancellation.
    /// Missing or failing cleanup is ignored and never replaces a primary error.
    pub fn dispose(&mut self) {
        if let Some(stub) = self.stub.take() {
            // Catch both property access and invocation. Never inspect or call
            // dispose on an entrypoint, whose unknown properties may be RPC.
            if let Ok(dispose) = method(&stub, "dispose") {
                let _ = dispose.call0(&stub);
            }
        }
    }
}

impl Drop for LoadedWorker {
    fn drop(&mut self) {
        self.dispose();
    }
}

/// Status, headers, and a bounded response body. Non-2xx status is not an error.
#[derive(Debug)]
pub struct BoundedResponse {
    pub status: u16,
    pub headers: Headers,
    pub body: Vec<u8>,
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
            // Observe rejection without delaying or replacing the
            // primary result, including when this future is dropped.
            spawn_local(async move {
                let _ = JsFuture::from(Promise::resolve(&result)).await;
            });
        }
        if let Ok(release) = method(&self.reader, "releaseLock") {
            let _ = release.call0(&self.reader);
        }
    }
}

fn method(target: &JsValue, name: &str) -> Result<Function> {
    Reflect::get(target, &JsValue::from_str(name))?
        .dyn_into()
        .map_err(Error::from)
}

fn set(target: &Object, name: &str, value: &JsValue) -> Result<()> {
    if !Reflect::set(target, &name.into(), value)? {
        return Err(Error::RustError(format!("cannot set Loader option {name}")));
    }
    Ok(())
}
