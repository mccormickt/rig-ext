//! Lexical analysis of a script before it runs: which tools it names and
//! whether it reaches the `tools` table in a way the scanner cannot resolve.
//!
//! The result is advisory input for a [`crate::ScriptPolicy`]. It is a
//! lexical hint with both false negatives and false positives, never a proof.
//! Do not use it to approve a denylist and then grant the entire catalog.
//! The explicit allowlist the policy returns is enforced on every call.

use std::collections::BTreeSet;

/// What a script names statically.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ScriptAnalysis {
    /// Tool names written literally as `tools.name`, `tools["name"]`,
    /// `tools?.name`, or `globalThis.tools.name`. Names are not checked
    /// against any catalog.
    pub tools: BTreeSet<String>,
    /// The script reaches `tools` with a computed key (`tools[expr]`) or uses
    /// the table as a value (`Object.keys(tools)`, `const t = tools`). The
    /// names it may call are then not known before it runs.
    pub dynamic_tool_access: bool,
    /// The script calls `searchTools` or `describeTool`.
    pub uses_discovery: bool,
}

impl ScriptAnalysis {
    /// True when the scanner found no dynamic access or scan limit.
    /// This does not prove that [`Self::tools`] contains every callable tool.
    pub fn is_static(&self) -> bool {
        !self.dynamic_tool_access
    }
}

/// Scan a script source. Comments and string literals are skipped; the code
/// inside template `${...}` holes is scanned. Regular-expression literals are
/// not recognized and can cause both missed names and spurious references.
/// Sources over 64 KiB or templates nested more than 64 levels stop analysis
/// and set `dynamic_tool_access`. No result proves complete tool coverage.
///
/// ```
/// use rig_codemode::analyze;
///
/// let review = analyze(r#"
///     await tools.lookup({ id: "order-17" });
///     const name = "archive";
///     await tools[name]({});
/// "#);
/// assert!(review.tools.contains("lookup"));
/// assert!(!review.tools.contains("archive"));
/// assert!(review.dynamic_tool_access);
/// ```
///
/// Use the result for review hints or [`crate::ScriptReview::grant_referenced`].
/// Enforce permissions with a [`crate::ScriptGrant`], not a source denylist.
pub fn analyze(source: &str) -> ScriptAnalysis {
    if source.len() > 64 * 1024 {
        return ScriptAnalysis {
            dynamic_tool_access: true,
            ..ScriptAnalysis::default()
        };
    }
    let mut scanner = Scanner {
        bytes: source.as_bytes(),
        pos: 0,
        nesting: 0,
        analysis: ScriptAnalysis::default(),
    };
    scanner.scan_code(false);
    scanner.analysis
}

struct Scanner<'a> {
    bytes: &'a [u8],
    pos: usize,
    nesting: usize,
    analysis: ScriptAnalysis,
}

fn is_ident_start(byte: u8) -> bool {
    byte.is_ascii_alphabetic() || byte == b'_' || byte == b'$' || byte >= 0x80
}

fn is_ident_continue(byte: u8) -> bool {
    is_ident_start(byte) || byte.is_ascii_digit()
}

impl Scanner<'_> {
    fn peek(&self, offset: usize) -> Option<u8> {
        self.bytes.get(self.pos + offset).copied()
    }

    /// Scan code until the end of input or, when `in_hole` is set, until the
    /// `}` that closes a template hole.
    fn scan_code(&mut self, in_hole: bool) {
        let mut depth = 0usize;
        while let Some(byte) = self.peek(0) {
            match byte {
                b'/' if self.peek(1) == Some(b'/') => self.skip_line_comment(),
                b'/' if self.peek(1) == Some(b'*') => self.skip_block_comment(),
                b'\'' | b'"' => {
                    self.skip_string(byte);
                }
                b'`' => {
                    self.skip_template();
                }
                b'{' => {
                    depth += 1;
                    self.pos += 1;
                }
                b'}' => {
                    if in_hole && depth == 0 {
                        self.pos += 1;
                        return;
                    }
                    depth = depth.saturating_sub(1);
                    self.pos += 1;
                }
                b if is_ident_start(b) => self.scan_identifier(),
                b if b.is_ascii_digit() => self.skip_while(is_ident_continue),
                _ => self.pos += 1,
            }
        }
    }

    fn skip_while(&mut self, keep: fn(u8) -> bool) {
        while self.peek(0).is_some_and(keep) {
            self.pos += 1;
        }
    }

    fn skip_whitespace(&mut self) {
        self.skip_while(|b| b.is_ascii_whitespace());
    }

    fn skip_line_comment(&mut self) {
        while let Some(byte) = self.peek(0) {
            self.pos += 1;
            if byte == b'\n' {
                break;
            }
        }
    }

    fn skip_block_comment(&mut self) {
        self.pos += 2;
        while let Some(byte) = self.peek(0) {
            if byte == b'*' && self.peek(1) == Some(b'/') {
                self.pos += 2;
                return;
            }
            self.pos += 1;
        }
    }

    /// Skip a quoted string. Returns its unescaped body when it has no escape
    /// sequence and is terminated.
    fn skip_string(&mut self, quote: u8) -> Option<String> {
        self.pos += 1;
        let start = self.pos;
        let mut escaped = false;
        while let Some(byte) = self.peek(0) {
            match byte {
                b'\\' => {
                    escaped = true;
                    self.pos += 2;
                }
                b'\n' => return None,
                b if b == quote => {
                    let body = &self.bytes[start..self.pos];
                    self.pos += 1;
                    return (!escaped)
                        .then(|| std::str::from_utf8(body).ok())
                        .flatten()
                        .map(str::to_owned);
                }
                _ => self.pos += 1,
            }
        }
        None
    }

    /// Skip a template literal, scanning the code inside `${...}` holes.
    /// Returns the body when it has no hole and no escape.
    fn skip_template(&mut self) -> Option<String> {
        if self.nesting >= 64 {
            self.analysis.dynamic_tool_access = true;
            self.pos = self.bytes.len();
            return None;
        }
        self.nesting += 1;
        let result = self.scan_template();
        self.nesting -= 1;
        result
    }

    fn scan_template(&mut self) -> Option<String> {
        self.pos += 1;
        let start = self.pos;
        let mut plain = true;
        while let Some(byte) = self.peek(0) {
            match byte {
                b'\\' => {
                    plain = false;
                    self.pos += 2;
                }
                b'$' if self.peek(1) == Some(b'{') => {
                    plain = false;
                    self.pos += 2;
                    self.scan_code(true);
                }
                b'`' => {
                    let body = &self.bytes[start..self.pos];
                    self.pos += 1;
                    return plain
                        .then(|| std::str::from_utf8(body).ok())
                        .flatten()
                        .map(str::to_owned);
                }
                _ => self.pos += 1,
            }
        }
        None
    }

    fn scan_identifier(&mut self) {
        let start = self.pos;
        let member_of_other = start > 0 && self.bytes[start - 1] == b'.';
        self.skip_while(is_ident_continue);
        let ident = &self.bytes[start..self.pos];
        match ident {
            b"eval" | b"Function" => self.analysis.dynamic_tool_access = true,
            b"tools" if !member_of_other => self.classify_tools_access(),
            b"globalThis" if !member_of_other => {
                let save = self.pos;
                self.skip_whitespace();
                if self.peek(0) == Some(b'[') {
                    self.analysis.dynamic_tool_access = true;
                }
                if self.peek(0) == Some(b'.') {
                    self.pos += 1;
                    self.skip_whitespace();
                    let name_start = self.pos;
                    self.skip_while(is_ident_continue);
                    if &self.bytes[name_start..self.pos] == b"tools" {
                        self.classify_tools_access();
                        return;
                    }
                }
                self.pos = save;
            }
            b"searchTools" | b"describeTool" if !member_of_other => {
                self.analysis.uses_discovery = true;
            }
            _ => {}
        }
    }

    /// The scanner is just past `tools`. Decide what the access names.
    fn classify_tools_access(&mut self) {
        let save = self.pos;
        self.skip_whitespace();
        if self.peek(0) == Some(b'?') && self.peek(1) == Some(b'.') {
            self.pos += 2;
            self.skip_whitespace();
        } else if self.peek(0) == Some(b'.') {
            self.pos += 1;
            self.skip_whitespace();
        } else if self.peek(0) != Some(b'[') {
            self.analysis.dynamic_tool_access = true;
            self.pos = save;
            return;
        }
        match self.peek(0) {
            Some(b'[') => {
                self.pos += 1;
                self.skip_whitespace();
                let literal = match self.peek(0) {
                    Some(quote @ (b'\'' | b'"')) => self.skip_string(quote),
                    Some(b'`') => self.skip_template(),
                    _ => None,
                };
                self.skip_whitespace();
                match literal {
                    Some(name) if self.peek(0) == Some(b']') => {
                        self.pos += 1;
                        self.analysis.tools.insert(name);
                    }
                    _ => self.analysis.dynamic_tool_access = true,
                }
            }
            Some(b) if is_ident_start(b) => {
                let start = self.pos;
                self.skip_while(is_ident_continue);
                if let Ok(name) = std::str::from_utf8(&self.bytes[start..self.pos]) {
                    self.analysis.tools.insert(name.to_owned());
                }
            }
            _ => self.analysis.dynamic_tool_access = true,
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    fn names(source: &str) -> Vec<String> {
        analyze(source).tools.into_iter().collect()
    }

    #[test]
    fn collects_literal_member_and_index_access() {
        let analysis = analyze(
            r#"
            const a = await tools.list_orders({ customer: "acme" });
            const b = await tools["order_details"]({ id: a[0].id });
            const c = await tools['cancel_order'].raw({ id: "x" });
            const d = await tools?.optional({});
            const e = await tools[`templated`]({});
            const f = await globalThis.tools.global_one({});
            "#,
        );
        assert_eq!(
            analysis.tools.into_iter().collect::<Vec<_>>(),
            [
                "cancel_order",
                "global_one",
                "list_orders",
                "optional",
                "order_details",
                "templated"
            ]
        );
        assert!(!analysis.dynamic_tool_access);
        assert!(!analysis.uses_discovery);
    }

    #[test]
    fn ignores_names_in_comments_and_strings() {
        let analysis = analyze(
            r#"
            // tools.commented({})
            /* tools["blocked"]() */
            const s = "tools.in_string()";
            const t = 'tools["in_single"]';
            const u = `tools.in_template`;
            await tools.real({});
            "#,
        );
        assert_eq!(analysis.tools.into_iter().collect::<Vec<_>>(), ["real"]);
        assert!(!analysis.dynamic_tool_access);
    }

    #[test]
    fn scans_code_inside_template_holes() {
        let analysis = analyze(r#"text(`total ${(await tools.sum({})).n} for ${name}`);"#);
        assert_eq!(analysis.tools.into_iter().collect::<Vec<_>>(), ["sum"]);
        assert!(!analysis.dynamic_tool_access);
    }

    #[test]
    fn nested_braces_inside_hole_do_not_end_the_template() {
        let analysis =
            analyze(r#"text(`${ {a: 1}.a } ${ (() => { return 2; })() }`); tools.after({});"#);
        assert_eq!(analysis.tools.into_iter().collect::<Vec<_>>(), ["after"]);
    }

    #[test]
    fn flags_computed_keys_and_value_use() {
        for source in [
            "await tools[name]({})",
            r#"await tools["a" + b]({})"#,
            "await tools[`${n}`]({})",
            "const t = tools; await t.x({})",
            "for (const k of Object.keys(tools)) {}",
            "await tools?.[name]({})",
        ] {
            let analysis = analyze(source);
            assert!(analysis.dynamic_tool_access, "{source}");
        }
    }

    #[test]
    fn member_named_tools_on_another_object_is_not_the_table() {
        let analysis = analyze("const r = await api.tools.list({}); await tools.mine({});");
        assert_eq!(analysis.tools.into_iter().collect::<Vec<_>>(), ["mine"]);
        assert!(!analysis.dynamic_tool_access);
    }

    #[test]
    fn identifiers_containing_tools_are_not_matched() {
        let analysis = analyze("const mytools = 1; const toolsy = 2; await tools_x.y({});");
        assert!(analysis.tools.is_empty());
        assert!(!analysis.dynamic_tool_access);
    }

    #[test]
    fn escaped_string_key_is_dynamic() {
        assert!(analyze(r#"await tools["a\u0062"]({})"#).dynamic_tool_access);
        assert!(names(r#"await tools["a\u0062"]({})"#).is_empty());
    }

    #[test]
    fn discovery_calls_are_flagged() {
        assert!(analyze("const r = searchTools('orders');").uses_discovery);
        assert!(analyze("describeTool('x')").uses_discovery);
        assert!(!analyze("obj.searchTools()").uses_discovery);
    }

    #[test]
    fn unterminated_input_does_not_panic() {
        for source in [
            "tools.",
            "tools[",
            "tools[\"abc",
            "`${tools.x(",
            "/* open",
            "\"open",
            "globalThis.",
            "tools",
        ] {
            let _ = analyze(source);
        }
        assert_eq!(names("tools.x("), ["x"]);
        assert!(analyze("tools").dynamic_tool_access);
    }

    #[test]
    fn deep_templates_and_large_sources_are_incomplete() {
        let opening = "`${".repeat(4000);
        assert!(analyze(&opening).dynamic_tool_access);
        assert!(analyze(&format!("{opening}0{}", "}`".repeat(4000))).dynamic_tool_access);
        assert!(analyze(&" ".repeat(64 * 1024 + 1)).dynamic_tool_access);
        assert!(!analyze(&" ".repeat(64 * 1024)).dynamic_tool_access);
        assert_eq!(names("`${`${tools.deep()}`}`"), ["deep"]);
    }

    #[test]
    fn indirect_access_is_advisory() {
        for source in [
            r#"globalThis["tools"].counter()"#,
            r#"eval("tools.counter()")"#,
            r#"new Function("return tools.counter()")()"#,
        ] {
            assert!(analyze(source).dynamic_tool_access, "{source}");
        }
        // A quote in a regular expression can hide the following tool access.
        let analysis = analyze(r#"/"/; tools.counter();"#);
        assert!(analysis.tools.is_empty());
    }
}
