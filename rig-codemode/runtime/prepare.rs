//! Retain only the exchange ABI and remove unreachable binding descriptors.

pub fn core(
    input: &std::path::Path,
    output: &std::path::Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut module = walrus::Module::from_file(input)?;
    let exports = [
        "memory",
        "exchange_abi_version",
        "exchange_alloc",
        "exchange_run",
        "exchange_output_ptr",
        "exchange_output_len",
    ];
    let removed: Vec<_> = module
        .exports
        .iter()
        .filter(|export| !exports.contains(&export.name.as_str()))
        .map(|export| export.id())
        .collect();
    for id in removed {
        module.exports.delete(id);
    }
    let customs: Vec<_> = module.customs.iter().map(|(id, _)| id).collect();
    for id in customs {
        module.customs.delete(id);
    }
    walrus::passes::gc::run(&mut module);
    // The binding crate's abort hook terminates this import-free instance.
    let throws: Vec<_> = module
        .imports
        .iter()
        .filter_map(|import| match import.kind {
            walrus::ImportKind::Function(id)
                if import.module == "./core_bg.js"
                    && import.name == "__wbg___wbindgen_throw_41e9ee4f547fc59a" =>
            {
                Some(id)
            }
            _ => None,
        })
        .collect();
    for id in throws {
        module.replace_imported_func(id, |(body, _)| {
            body.unreachable();
        })?;
    }
    if module.imports.iter().next().is_some() {
        let imports: Vec<_> = module
            .imports
            .iter()
            .map(|import| format!("{}::{}", import.module, import.name))
            .collect();
        return Err(format!("QuickJS core must have no imports: {imports:?}").into());
    }
    module.emit_wasm_file(output)?;
    Ok(())
}
