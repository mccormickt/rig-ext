//! Import-free exchange ABI. Each instance serves exactly one exchange.
//! Pointers are offsets into exported WASM memory, never host pointers.

use std::cell::RefCell;

use rig_codemode::worker_runtime::MAX_TRANSPORT;

thread_local! {
    static INPUT: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
    static OUTPUT: RefCell<String> = const { RefCell::new(String::new()) };
}

#[unsafe(no_mangle)]
pub extern "C" fn exchange_abi_version() -> u32 {
    1
}

#[unsafe(no_mangle)]
pub extern "C" fn exchange_alloc(len: u32) -> u32 {
    if len == 0 || len as usize > MAX_TRANSPORT {
        return 0;
    }
    INPUT.with_borrow_mut(|input| {
        input.resize(len as usize, 0);
        input.as_mut_ptr() as u32
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn exchange_run() -> u32 {
    INPUT.with_borrow(|input| {
        let Ok(body) = std::str::from_utf8(input) else {
            return 422;
        };
        match rig_codemode::worker_runtime::exchange(body, crate::execute_job) {
            Ok(body) => {
                OUTPUT.with_borrow_mut(|output| *output = body);
                200
            }
            Err(rig_codemode::replay::ReplayError::BudgetExceeded) => 429,
            Err(_) => 422,
        }
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn exchange_output_ptr() -> u32 {
    OUTPUT.with_borrow(|output| output.as_ptr() as u32)
}

#[unsafe(no_mangle)]
pub extern "C" fn exchange_output_len() -> u32 {
    OUTPUT.with_borrow(|output| output.len() as u32)
}
