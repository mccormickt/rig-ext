//! Native replay through an embedded, import-free QuickJS-WASM module.
//!
//! No JavaScript host, WASI, filesystem, network, or tool callbacks are linked.
//! Each exchange gets a fresh Store and instance. Dropping a planning future
//! drops its suspended WASM execution; it cannot cancel external tool effects.

use std::time::{Duration, Instant};

use wasmtime::{Config, Engine, Instance, Module, Store, StoreLimitsBuilder};

use crate::projection::bounded_json_with_depth;
use crate::replay::protocol::{
    CheckedPlanner, ExchangeBudget, ReplayRunner, RunnerRequest, RunnerResponse, decode_response,
};
use crate::replay::{ReplayError, ReplayFuture, ReplayInput, ReplayPlanner};
use rig_core::wasm_compat::WasmBoxedFuture;

const WASM: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/quickjs-core.wasm"));
const MAX_TRANSPORT: usize = 16 * 1024 * 1024;
const FUEL_SLICE: u64 = 100_000;
const STACK_BYTES: usize = 512 * 1024;

/// Host ceilings for one planning operation, including reconstructed prefixes.
#[derive(Debug, Clone, Copy)]
pub struct WasmtimeBudget {
    /// Aggregate encoded requests and received responses.
    pub bytes: usize,
    /// Aggregate Wasmtime instruction fuel, including initialization and serialization.
    pub fuel: u64,
    /// Total WASM linear memory per exchange, including Rust and QuickJS allocations.
    /// This does not cap the host process or Wasmtime's compiled code.
    pub memory_bytes: usize,
    /// Host elapsed time. Also capped by the script's `Limits::wall_time`.
    pub elapsed: Duration,
}

impl Default for WasmtimeBudget {
    fn default() -> Self {
        Self {
            bytes: 64 * 1024 * 1024,
            fuel: 1_000_000_000,
            memory_bytes: 128 * 1024 * 1024,
            elapsed: Duration::from_secs(30),
        }
    }
}

/// A reusable compiled module with disposable interpreters. Requires a Tokio timer
/// when planning. Construction compiles trusted embedded bytes, outside script time.
pub struct WasmtimeReplayPlanner {
    planner: CheckedPlanner<WasmtimeRunner>,
    identity: String,
    elapsed: Duration,
}

impl WasmtimeReplayPlanner {
    /// Compile the packaged module once. No files, downloads, or external runtime
    /// installation are required. Keep this planner for subsequent executions.
    pub fn new(budget: WasmtimeBudget) -> Result<Self, ReplayError> {
        if budget.memory_bytes == 0 || budget.memory_bytes as u128 > u32::MAX as u128 {
            return Err(ReplayError::BudgetExceeded);
        }
        let mut config = Config::new();
        config.consume_fuel(true).max_wasm_stack(STACK_BYTES);
        let engine = Engine::new(&config).map_err(runtime_error)?;
        let module = Module::new(&engine, WASM).map_err(runtime_error)?;
        if module.imports().next().is_some() {
            return Err(ReplayError::Runner(
                "QuickJS core must have no imports".into(),
            ));
        }
        let identity = format!(
            "wasmtime-48.0.8:adapter-v1:{}:slice={FUEL_SLICE}:stack={STACK_BYTES}:transport={MAX_TRANSPORT}:memory={}:bytes={}:fuel={}:elapsed-ns={}",
            include_str!(concat!(env!("OUT_DIR"), "/core-identity.txt")).trim(),
            budget.memory_bytes,
            budget.bytes,
            budget.fuel,
            budget.elapsed.as_nanos(),
        );
        Ok(Self {
            planner: CheckedPlanner::new(
                WasmtimeRunner {
                    module,
                    memory_bytes: budget.memory_bytes,
                },
                ExchangeBudget {
                    bytes: budget.bytes,
                    work: budget.fuel,
                    elapsed: budget.elapsed,
                },
            ),
            identity,
            elapsed: budget.elapsed,
        })
    }
}

impl ReplayPlanner for WasmtimeReplayPlanner {
    fn runtime_identity(&self) -> &str {
        &self.identity
    }

    fn plan<'a>(&'a self, input: ReplayInput<'a>) -> ReplayFuture<'a> {
        Box::pin(async move {
            // Bound the entire plan, including host prefix encoding and validation.
            let started = Instant::now();
            let elapsed = input.limits.wall_time.min(self.elapsed);
            let result = tokio::time::timeout(elapsed, self.planner.plan(input))
                .await
                .map_err(|_| ReplayError::BudgetExceeded)?;
            if started.elapsed() > elapsed {
                return Err(ReplayError::BudgetExceeded);
            }
            result
        })
    }
}

struct WasmtimeRunner {
    module: Module,
    memory_bytes: usize,
}

impl ReplayRunner for WasmtimeRunner {
    fn run<'a>(
        &'a self,
        request: RunnerRequest,
    ) -> WasmBoxedFuture<'a, Result<RunnerResponse, ReplayError>> {
        Box::pin(async move {
            let started = Instant::now();
            let elapsed = request.budget.elapsed;
            let result = tokio::time::timeout(elapsed, self.exchange(request))
                .await
                .map_err(|_| ReplayError::BudgetExceeded)??;
            if started.elapsed() > elapsed {
                return Err(ReplayError::BudgetExceeded);
            }
            Ok(result)
        })
    }
}

impl WasmtimeRunner {
    async fn exchange(&self, request: RunnerRequest) -> Result<RunnerResponse, ReplayError> {
        let started = Instant::now();
        let body = bounded_json_with_depth(&request, MAX_TRANSPORT, 128)
            .map_err(|_| ReplayError::BudgetExceeded)?;
        let limits = StoreLimitsBuilder::new()
            .memory_size(self.memory_bytes)
            .memories(1)
            .tables(1)
            .instances(1)
            .table_elements(10_000)
            .trap_on_grow_failure(true)
            .build();
        let mut store = Store::new(self.module.engine(), limits);
        store.limiter(|limits| limits);
        store.set_fuel(request.budget.work).map_err(runtime_error)?;
        store
            .fuel_async_yield_interval(Some(FUEL_SLICE))
            .map_err(runtime_error)?;
        let instance = Instance::new_async(&mut store, &self.module, &[])
            .await
            .map_err(runtime_error)?;
        let version = instance
            .get_typed_func::<(), u32>(&mut store, "exchange_abi_version")
            .map_err(runtime_error)?;
        if version
            .call_async(&mut store, ())
            .await
            .map_err(runtime_error)?
            != 1
        {
            return Err(ReplayError::Protocol);
        }
        let memory = instance
            .get_memory(&mut store, "memory")
            .ok_or(ReplayError::Protocol)?;
        let alloc = instance
            .get_typed_func::<u32, u32>(&mut store, "exchange_alloc")
            .map_err(runtime_error)?;
        let ptr = alloc
            .call_async(&mut store, body.len() as u32)
            .await
            .map_err(runtime_error)?;
        if ptr == 0 {
            return Err(ReplayError::BudgetExceeded);
        }
        memory
            .write(&mut store, ptr as usize, body.as_bytes())
            .map_err(|_| ReplayError::Protocol)?;
        let run = instance
            .get_typed_func::<(), u32>(&mut store, "exchange_run")
            .map_err(runtime_error)?;
        match run
            .call_async(&mut store, ())
            .await
            .map_err(runtime_error)?
        {
            200 => {}
            429 => return Err(ReplayError::BudgetExceeded),
            _ => return Err(ReplayError::Protocol),
        }
        let len = instance
            .get_typed_func::<(), u32>(&mut store, "exchange_output_len")
            .map_err(runtime_error)?
            .call_async(&mut store, ())
            .await
            .map_err(runtime_error)? as usize;
        if len > MAX_TRANSPORT.min(request.budget.bytes) {
            return Err(ReplayError::BudgetExceeded);
        }
        let ptr = instance
            .get_typed_func::<(), u32>(&mut store, "exchange_output_ptr")
            .map_err(runtime_error)?
            .call_async(&mut store, ())
            .await
            .map_err(runtime_error)? as usize;
        // Validate guest offsets and lengths before copying or parsing host data.
        let end = ptr.checked_add(len).ok_or(ReplayError::Protocol)?;
        let body = memory
            .data(&store)
            .get(ptr..end)
            .ok_or(ReplayError::Protocol)?;
        let mut response = decode_response(body, request.limits)?;
        response.usage.response_bytes = len;
        response.usage.work = request.budget.work - store.get_fuel().map_err(runtime_error)?;
        response.usage.elapsed = started.elapsed();
        Ok(response)
    }
}

fn runtime_error(error: wasmtime::Error) -> ReplayError {
    if error.downcast_ref::<wasmtime::Trap>() == Some(&wasmtime::Trap::OutOfFuel) {
        ReplayError::BudgetExceeded
    } else {
        ReplayError::Runner(error.to_string())
    }
}
