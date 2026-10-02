//! Durable Object storage with synchronous transactions.

use worker::{
    SqlStorage, State,
    js_sys::{Function, Reflect},
    wasm_bindgen::{JsCast, JsValue, closure::ScopedClosure},
    worker_sys,
};

/// The storage of one Durable Object.
///
/// workers-rs 0.8.3 does not expose `ctx.storage.transactionSync()`. This
/// handle adds it, so a group of SQL statements commits as one SQLite
/// transaction. celld 0.6.0 and later nest a `transactionSync()` call inside an
/// open one as a savepoint, so an application can wrap index writes and its
/// own writes in one outer transaction.
#[derive(Clone, Debug)]
pub struct CellStorage {
    state: JsValue,
}

impl From<State> for CellStorage {
    fn from(state: State) -> Self {
        Self {
            state: state._inner().into(),
        }
    }
}

impl CellStorage {
    /// Return the Durable Object state that this handle was made from.
    pub fn state(&self) -> State {
        State::from(
            self.state
                .clone()
                .unchecked_into::<worker_sys::DurableObjectState>(),
        )
    }

    /// Return the SQL API of this Durable Object.
    pub fn sql(&self) -> SqlStorage {
        self.state().storage().sql()
    }

    /// Run `f` in one `transactionSync()` call.
    ///
    /// When `f` returns an error, the runtime rolls back every write that `f`
    /// made and this method returns that error. When `f` succeeds, all its
    /// writes commit together. A call inside an open transaction becomes a
    /// nested savepoint.
    ///
    /// Return recoverable failures as `Err`. A panic in `f` traps the Wasm
    /// instance. The runtime then rolls back the database writes, but Rust
    /// destructors do not run and in-memory state is not restored.
    pub fn transaction_sync<T, E>(&self, f: impl FnOnce() -> Result<T, E>) -> Result<T, E>
    where
        E: From<worker::Error>,
    {
        let storage = Reflect::get(&self.state, &JsValue::from_str("storage"))
            .map_err(worker::Error::from)?;
        let transaction_sync = Reflect::get(&storage, &JsValue::from_str("transactionSync"))
            .and_then(JsCast::dyn_into::<Function>)
            .map_err(worker::Error::from)?;
        let mut f = Some(f);
        let mut outcome = None;
        let call = {
            let mut callback = || {
                let f = f
                    .take()
                    .ok_or_else(|| JsValue::from_str("transactionSync callback ran twice"))?;
                let result = f();
                let failed = result.is_err();
                outcome = Some(result);
                if failed {
                    // The runtime rolls back the transaction when the callback throws.
                    Err(JsValue::from_str("rig-celld: rolled back after an error"))
                } else {
                    Ok(())
                }
            };
            // `transactionSync()` runs the callback before it returns, so a
            // borrowed closure outlives every call.
            let callback =
                ScopedClosure::<dyn FnMut() -> Result<(), JsValue>>::borrow_mut(&mut callback);
            transaction_sync.call1(&storage, callback.as_ref())
        };
        match (call, outcome) {
            (_, Some(Err(error))) => Err(error),
            (Ok(_), Some(Ok(value))) => Ok(value),
            (Err(error), _) => Err(worker::Error::from(error).into()),
            (Ok(_), None) => Err(worker::Error::RustError(
                "transactionSync returned without running its callback".to_owned(),
            )
            .into()),
        }
    }
}
