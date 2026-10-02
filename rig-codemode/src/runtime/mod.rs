//! Script runtime backends. Each backend runs one script on an isolated
//! worker, speaks the bridge protocol in [`crate::bridge`], and never exposes
//! engine types outside this module.

#[cfg(feature = "quickjs")]
pub(crate) mod quickjs;

/// The worker could not be started.
#[derive(Debug, thiserror::Error)]
#[error("could not start the script worker: {0}")]
pub struct SpawnError(#[from] pub(crate) std::io::Error);

/// Prelude shared by every backend: builds the frozen null-prototype `tools`
/// table and the script-facing helpers from the private `__host` object, then
/// removes `__host` from the global scope.
pub(crate) const PRELUDE: &str = r#"
(function (host) {
  "use strict";
  const define = (target, key, value) =>
    Object.defineProperty(target, key, { value, enumerable: true, writable: false, configurable: false });
  const tools = Object.create(null);
  for (const name of host.names()) {
    const call = (args) => host.invoke(name, args === undefined ? {} : args, false);
    const raw = (args) => host.invoke(name, args === undefined ? {} : args, true);
    define(call, "raw", Object.freeze(raw));
    define(call, "toolName", name);
    define(tools, name, Object.freeze(call));
  }
  Object.freeze(tools);
  define(globalThis, "tools", tools);
  define(globalThis, "text", (value) => host.text(value));
  define(globalThis, "searchTools", (query, options) =>
    host.searchTools(query === undefined || query === null ? "" : String(query),
      options && options.limit !== undefined && options.limit !== null ? Number(options.limit) : undefined));
  define(globalThis, "describeTool", (name) => host.describeTool(String(name)));
})(globalThis.__host);
delete globalThis.__host;
"#;

/// Wrap the model's code as an async function body. The wrapper occupies one
/// line, so guest line numbers are one greater than source line numbers.
pub(crate) fn wrap_source(code: &str) -> String {
    format!("(async () => {{\n{code}\n}})()")
}

/// Guest file name used for the model's code; stack frames cite it.
pub(crate) const SCRIPT_FILENAME: &str = "script";

/// Parse the innermost `script:LINE[:COLUMN]` position out of a guest stack
/// trace and translate it to source coordinates.
pub(crate) fn locate(stack: &str) -> (Option<u32>, Option<u32>) {
    let marker = format!("{SCRIPT_FILENAME}:");
    for line in stack.lines() {
        let Some(index) = line.find(&marker) else {
            continue;
        };
        let rest = &line[index + marker.len()..];
        let mut numbers = rest
            .trim_end_matches(')')
            .split(':')
            .map(|part| part.trim().parse::<u32>().ok());
        let guest_line = numbers.next().flatten();
        let column = numbers.next().flatten();
        if let Some(guest_line) = guest_line {
            return (Some(guest_line.saturating_sub(1).max(1)), column);
        }
    }
    (None, None)
}

/// Bounded `text()` writer. Strings are cut at a character boundary when the
/// budget runs out; later writes are dropped.
pub(crate) fn write_text(output: &mut crate::report::ScriptOutput, limit: usize, text: &str) {
    if output.truncated {
        return;
    }
    let remaining = limit.saturating_sub(output.text.len());
    if text.len() < remaining {
        output.text.push_str(text);
        output.text.push('\n');
        return;
    }
    let mut cut = remaining.min(text.len());
    while cut > 0 && !text.is_char_boundary(cut) {
        cut -= 1;
    }
    output.text.push_str(&text[..cut]);
    output.truncated = true;
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::report::ScriptOutput;

    #[test]
    fn locate_translates_wrapper_offset() {
        assert_eq!(
            locate("    at <anonymous> (script:3:7)\n    at run (native)"),
            (Some(2), Some(7))
        );
        assert_eq!(locate("    at script:1"), (Some(1), None));
        assert_eq!(locate("    at other:4:4"), (None, None));
    }

    #[test]
    fn write_text_truncates_on_char_boundary() {
        let mut output = ScriptOutput::default();
        write_text(&mut output, 2, "héllo");
        assert!(output.truncated);
        assert_eq!(output.text, "h", "byte 2 splits `é`, so the cut moves back");
        write_text(&mut output, 2, "more");
        assert_eq!(output.text, "h");

        let mut exact = ScriptOutput::default();
        write_text(&mut exact, 4, "abc");
        assert_eq!(exact.text, "abc\n");
        assert!(!exact.truncated);
        write_text(&mut exact, 4, "d");
        assert!(exact.truncated);
        assert_eq!(exact.text, "abc\n");
    }
}
