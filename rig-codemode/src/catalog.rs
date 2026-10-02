//! The approved, script-callable tool catalog: exact names, schemas,
//! lexical search, and bounded TypeScript declarations for prompts.
//!
//! A [`Catalog`] is a fixed snapshot. Scripts discover entries with
//! `searchTools` and `describeTool`; execution rejects names outside the
//! snapshot. Membership is not authorization: the host dispatcher still
//! decides every call.

use std::collections::{BTreeMap, HashMap};
use std::fmt::Write as _;

use rig_core::completion::ToolDefinition;
use serde::{Deserialize, Serialize};

/// Longest accepted tool name, in bytes.
pub const MAX_NAME_BYTES: usize = 128;

pub(crate) const MAX_QUERY_BYTES: usize = 4096;
const MAX_QUERY_TERMS: usize = 32;

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DiscoveryEntry<'a> {
    name: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    namespace: Option<&'a str>,
    description: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    input_schema: Option<&'a serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    output_schema: Option<&'a serde_json::Value>,
}

/// Whether a tool's declaration is rendered inline in the outer prompt or
/// left to script discovery.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Presentation {
    /// Rendered in [`Catalog::render_declarations`] while the byte budget allows.
    Inline,
    /// Available to scripts through `searchTools` / `describeTool` only.
    Deferred,
}

/// One script-callable tool.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CatalogEntry {
    name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    namespace: Option<String>,
    description: String,
    input_schema: serde_json::Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    output_schema: Option<serde_json::Value>,
    presentation: Presentation,
}

impl CatalogEntry {
    /// An inline entry with the given exact name, description, and JSON Schema
    /// for its arguments.
    pub fn new(
        name: impl Into<String>,
        description: impl Into<String>,
        input_schema: serde_json::Value,
    ) -> Self {
        Self {
            name: name.into(),
            namespace: None,
            description: description.into(),
            input_schema,
            output_schema: None,
            presentation: Presentation::Inline,
        }
    }

    /// An entry from a Rig tool definition. The definition has no output
    /// schema; add one with [`Self::with_output_schema`] when known.
    pub fn from_definition(definition: &ToolDefinition) -> Self {
        Self::new(
            definition.name.clone(),
            definition.description.clone(),
            definition.parameters.clone(),
        )
    }

    /// Group the entry under a namespace for discovery. The name is not
    /// changed; scripts still call the exact name.
    pub fn with_namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = Some(namespace.into());
        self
    }

    /// Declare the JSON Schema of successful structured results.
    pub fn with_output_schema(mut self, schema: serde_json::Value) -> Self {
        self.output_schema = Some(schema);
        self
    }

    /// Set how the entry is presented to the model.
    pub fn with_presentation(mut self, presentation: Presentation) -> Self {
        self.presentation = presentation;
        self
    }

    /// Exact script-callable name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Discovery namespace, if any.
    pub fn namespace(&self) -> Option<&str> {
        self.namespace.as_deref()
    }

    /// Model-facing description. Treat it as untrusted content.
    pub fn description(&self) -> &str {
        &self.description
    }

    /// JSON Schema for arguments.
    pub fn input_schema(&self) -> &serde_json::Value {
        &self.input_schema
    }

    /// JSON Schema for successful structured results, if declared.
    pub fn output_schema(&self) -> Option<&serde_json::Value> {
        self.output_schema.as_ref()
    }

    /// Prompt presentation.
    pub fn presentation(&self) -> Presentation {
        self.presentation
    }

    /// The JSON object scripts receive from `describeTool`.
    #[cfg_attr(not(feature = "quickjs"), allow(dead_code))]
    pub(crate) fn describe(&self) -> DiscoveryEntry<'_> {
        DiscoveryEntry {
            input_schema: Some(&self.input_schema),
            output_schema: self.output_schema.as_ref(),
            ..self.summarize()
        }
    }

    /// The JSON object scripts receive from `searchTools`.
    #[cfg_attr(not(feature = "quickjs"), allow(dead_code))]
    pub(crate) fn summarize(&self) -> DiscoveryEntry<'_> {
        DiscoveryEntry {
            name: &self.name,
            namespace: self.namespace.as_deref(),
            description: &self.description,
            input_schema: None,
            output_schema: None,
        }
    }
}

/// A catalog could not be built.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CatalogError {
    /// A name is empty, too long, or contains whitespace or control characters.
    #[error("invalid tool name {name:?}: {reason}")]
    InvalidName {
        /// The rejected name.
        name: String,
        /// Why it was rejected.
        reason: &'static str,
    },
    /// Two entries share one exact name.
    #[error("duplicate tool name {0:?}")]
    DuplicateName(String),
}

/// A fixed snapshot of script-callable tools.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "Vec<CatalogEntry>", into = "Vec<CatalogEntry>")]
pub struct Catalog {
    entries: Vec<CatalogEntry>,
    index: HashMap<String, usize>,
    documents: Vec<SearchDocument>,
}

#[derive(Debug, Clone, PartialEq)]
struct SearchDocument {
    terms: BTreeMap<String, usize>,
    names: Vec<String>,
    length: usize,
}

impl Catalog {
    /// Build a catalog, rejecting invalid or duplicate names. No aliases are
    /// created: `a-b` and `a_b` are different tools.
    pub fn new(entries: impl IntoIterator<Item = CatalogEntry>) -> Result<Self, CatalogError> {
        let entries: Vec<CatalogEntry> = entries.into_iter().collect();
        let mut index = HashMap::with_capacity(entries.len());
        for (position, entry) in entries.iter().enumerate() {
            validate_name(&entry.name)?;
            if index.insert(entry.name.clone(), position).is_some() {
                return Err(CatalogError::DuplicateName(entry.name.clone()));
            }
        }
        let documents = entries
            .iter()
            .map(|entry| {
                let mut terms = BTreeMap::new();
                for term in tokenize(entry.namespace.as_deref().unwrap_or(""))
                    .into_iter()
                    .chain(tokenize(&entry.description))
                {
                    *terms.entry(term).or_insert(0) += 1;
                }
                let length = terms.values().sum();
                SearchDocument {
                    terms,
                    names: tokenize(&entry.name),
                    length,
                }
            })
            .collect();
        Ok(Self {
            entries,
            index,
            documents,
        })
    }

    /// Entries in insertion order.
    pub fn entries(&self) -> &[CatalogEntry] {
        &self.entries
    }

    /// Whether the catalog has no entries.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Number of entries.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Look up an exact name.
    pub fn get(&self, name: &str) -> Option<&CatalogEntry> {
        self.index.get(name).and_then(|&i| self.entries.get(i))
    }

    /// Whether an exact name is present.
    pub fn contains(&self, name: &str) -> bool {
        self.index.contains_key(name)
    }

    /// Rank entries against a query with a bounded lexical BM25 scorer over
    /// names, namespaces, and descriptions. Ties break on exact name. Only
    /// entries with a positive score are returned, at most 50. Queries over
    /// 4096 bytes or 32 tokens return no results; repeated terms count once.
    pub fn search(&self, query: &str, limit: usize) -> Vec<&CatalogEntry> {
        self.search_checked(query, limit, || false)
            .unwrap_or_default()
    }

    pub(crate) fn search_checked(
        &self,
        query: &str,
        limit: usize,
        mut interrupted: impl FnMut() -> bool,
    ) -> Result<Vec<&CatalogEntry>, &'static str> {
        if query.len() > MAX_QUERY_BYTES {
            return Err("search query exceeds 4096 bytes");
        }
        let tokens = || {
            query
                .split(|c: char| !c.is_alphanumeric())
                .filter(|t| !t.is_empty())
        };
        if tokens().take(MAX_QUERY_TERMS + 1).count() > MAX_QUERY_TERMS {
            return Err("search query exceeds 32 tokens");
        }
        let mut query_terms = tokenize(query);
        query_terms.sort_unstable();
        query_terms.dedup();
        let limit = limit.min(50);
        if query_terms.is_empty() || limit == 0 {
            return Ok(Vec::new());
        }
        let mut frequencies = vec![0usize; query_terms.len()];
        let mut total_length = 0usize;
        for doc in &self.documents {
            if interrupted() {
                return Err("search interrupted");
            }
            total_length += doc.length;
            for (term, frequency) in query_terms.iter().zip(&mut frequencies) {
                if doc.terms.contains_key(term) || doc.names.contains(term) {
                    *frequency += 1;
                }
            }
        }
        let total_docs = self.documents.len() as f64;
        let average_length = total_length as f64 / total_docs.max(1.0);
        let idfs: Vec<f64> = frequencies
            .into_iter()
            .map(|df| {
                let df = df as f64;
                ((total_docs - df + 0.5) / (df + 0.5) + 1.0).ln()
            })
            .collect();
        const K1: f64 = 1.2;
        const B: f64 = 0.75;
        let mut scored: Vec<(f64, &CatalogEntry)> = Vec::with_capacity(limit + 1);
        for (doc, entry) in self.documents.iter().zip(&self.entries) {
            if interrupted() {
                return Err("search interrupted");
            }
            let mut score = 0.0;
            for (term, idf) in query_terms.iter().zip(&idfs) {
                if doc.names.contains(term) {
                    score += idf * (K1 + 1.0);
                }
                if let Some(&tf) = doc.terms.get(term) {
                    let tf = tf as f64;
                    let norm = K1 * (1.0 - B + B * doc.length as f64 / average_length.max(1.0));
                    score += idf * (tf * (K1 + 1.0)) / (tf + norm);
                }
            }
            if score > 0.0 {
                scored.push((score, entry));
                scored
                    .sort_by(|(a, ea), (b, eb)| b.total_cmp(a).then_with(|| ea.name.cmp(&eb.name)));
                scored.truncate(limit);
            }
        }
        Ok(scored.into_iter().map(|(_, e)| e).collect())
    }

    /// Render TypeScript declarations for inline entries within `byte_budget`
    /// bytes, followed by a note about any entries left to discovery. JSON
    /// Schema stays the validation source of truth; unsupported schema
    /// features render as `unknown`.
    pub fn render_declarations(&self, byte_budget: usize) -> String {
        let mut rendered = String::from("declare const tools: {\n");
        if byte_budget < rendered.len() + 3 {
            return String::new();
        }
        let footer_reserve = 160;
        let mut omitted = 0usize;
        let mut deferred = 0usize;
        for entry in &self.entries {
            if entry.presentation == Presentation::Deferred {
                deferred += 1;
                continue;
            }
            let declaration = render_entry(entry);
            if rendered.len() + declaration.len() + footer_reserve > byte_budget {
                omitted += 1;
                continue;
            }
            rendered.push_str(&declaration);
        }
        rendered.push_str("};\n");
        let hidden = omitted + deferred;
        if hidden > 0 {
            let note = format!(
                "// {hidden} more tool(s) are callable but not listed. \
                 Use searchTools(query) and describeTool(name) to find them.\n"
            );
            if rendered.len() + note.len() <= byte_budget {
                rendered.push_str(&note);
            }
        }
        rendered
    }
}

impl TryFrom<Vec<CatalogEntry>> for Catalog {
    type Error = CatalogError;

    fn try_from(entries: Vec<CatalogEntry>) -> Result<Self, Self::Error> {
        Self::new(entries)
    }
}

impl From<Catalog> for Vec<CatalogEntry> {
    fn from(catalog: Catalog) -> Self {
        catalog.entries
    }
}

fn validate_name(name: &str) -> Result<(), CatalogError> {
    let reject = |reason| CatalogError::InvalidName {
        name: name.to_string(),
        reason,
    };
    if name.is_empty() {
        return Err(reject("name is empty"));
    }
    if name.len() > MAX_NAME_BYTES {
        return Err(reject("name is longer than 128 bytes"));
    }
    if name.chars().any(|c| c.is_whitespace() || c.is_control()) {
        return Err(reject("name contains whitespace or control characters"));
    }
    Ok(())
}

fn tokenize(text: &str) -> Vec<String> {
    text.split(|c: char| !c.is_alphanumeric())
        .filter(|t| !t.is_empty())
        .map(|t| t.to_lowercase())
        .collect()
}

fn render_entry(entry: &CatalogEntry) -> String {
    let mut out = String::new();
    if !entry.description.is_empty() {
        let description = entry.description.replace("*/", "* /");
        let _ = writeln!(
            out,
            "  /** {} */",
            description.lines().collect::<Vec<_>>().join(" ")
        );
    }
    let args = render_type(&entry.input_schema, 0);
    let result = entry
        .output_schema
        .as_ref()
        .map(|schema| render_type(schema, 0))
        .unwrap_or_else(|| "unknown".to_string());
    let _ = writeln!(
        out,
        "  {}(args: {args}): Promise<{result}>;",
        serde_json::to_string(&entry.name).unwrap_or_default()
    );
    out
}

const MAX_TYPE_DEPTH: usize = 6;

/// Render a JSON Schema fragment as a TypeScript type. Unknown or unsupported
/// constructs render as `unknown`; nothing is invented.
pub(crate) fn render_type(schema: &serde_json::Value, depth: usize) -> String {
    use serde_json::Value;
    if depth > MAX_TYPE_DEPTH {
        return "unknown".into();
    }
    let Value::Object(object) = schema else {
        return match schema {
            Value::Bool(true) => "unknown".into(),
            Value::Bool(false) => "never".into(),
            _ => "unknown".into(),
        };
    };
    if let Some(Value::Array(values)) = object.get("enum") {
        let literals: Vec<String> = values
            .iter()
            .map(|v| serde_json::to_string(v).unwrap_or_else(|_| "unknown".into()))
            .collect();
        if !literals.is_empty() {
            return literals.join(" | ");
        }
    }
    if let Some(value) = object.get("const") {
        return serde_json::to_string(value).unwrap_or_else(|_| "unknown".into());
    }
    for key in ["anyOf", "oneOf"] {
        if let Some(Value::Array(variants)) = object.get(key) {
            let rendered: Vec<String> =
                variants.iter().map(|v| render_type(v, depth + 1)).collect();
            if !rendered.is_empty() {
                return rendered.join(" | ");
            }
        }
    }
    let types: Vec<&str> = match object.get("type") {
        Some(Value::String(t)) => vec![t.as_str()],
        Some(Value::Array(ts)) => ts.iter().filter_map(Value::as_str).collect(),
        _ => {
            if object.contains_key("properties") {
                vec!["object"]
            } else if object.contains_key("items") {
                vec!["array"]
            } else {
                return "unknown".into();
            }
        }
    };
    let rendered: Vec<String> = types
        .iter()
        .map(|t| match *t {
            "string" => "string".to_string(),
            "number" | "integer" => "number".to_string(),
            "boolean" => "boolean".to_string(),
            "null" => "null".to_string(),
            "array" => {
                let items = object
                    .get("items")
                    .map(|items| render_type(items, depth + 1))
                    .unwrap_or_else(|| "unknown".into());
                if items.contains(' ') {
                    format!("Array<{items}>")
                } else {
                    format!("{items}[]")
                }
            }
            "object" => render_object(object, depth),
            _ => "unknown".to_string(),
        })
        .collect();
    if rendered.is_empty() {
        "unknown".into()
    } else {
        rendered.join(" | ")
    }
}

fn render_object(object: &serde_json::Map<String, serde_json::Value>, depth: usize) -> String {
    use serde_json::Value;
    let required: Vec<&str> = object
        .get("required")
        .and_then(Value::as_array)
        .map(|r| r.iter().filter_map(Value::as_str).collect())
        .unwrap_or_default();
    let Some(Value::Object(properties)) = object.get("properties") else {
        return match object.get("additionalProperties") {
            Some(Value::Bool(false)) => "{}".into(),
            Some(schema @ Value::Object(_)) => {
                format!("Record<string, {}>", render_type(schema, depth + 1))
            }
            _ => "Record<string, unknown>".into(),
        };
    };
    if properties.is_empty() {
        return "{}".into();
    }
    let mut fields = Vec::with_capacity(properties.len());
    for (name, schema) in properties {
        let optional = if required.contains(&name.as_str()) {
            ""
        } else {
            "?"
        };
        let key = if is_identifier(name) {
            name.clone()
        } else {
            serde_json::to_string(name).unwrap_or_default()
        };
        let mut field = String::new();
        if let Some(Value::String(description)) = schema.get("description") {
            let _ = write!(
                field,
                "/** {} */ ",
                description
                    .replace("*/", "* /")
                    .lines()
                    .collect::<Vec<_>>()
                    .join(" ")
            );
        }
        let _ = write!(field, "{key}{optional}: {}", render_type(schema, depth + 1));
        fields.push(field);
    }
    format!("{{ {} }}", fields.join("; "))
}

fn is_identifier(name: &str) -> bool {
    let mut chars = name.chars();
    match chars.next() {
        Some(c) if c.is_alphabetic() || c == '_' || c == '$' => {}
        _ => return false,
    }
    chars.all(|c| c.is_alphanumeric() || c == '_' || c == '$')
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]
mod tests {
    use super::*;
    use serde_json::json;

    fn entry(name: &str, description: &str) -> CatalogEntry {
        CatalogEntry::new(name, description, json!({"type": "object"}))
    }

    #[test]
    fn search_bounds_queries_and_checks_both_scans() {
        let catalog =
            Catalog::new((0..10000).map(|i| entry(&format!("tool_{i}"), "common alpha beta")))
                .unwrap();
        assert!(
            catalog
                .search_checked(&"x".repeat(MAX_QUERY_BYTES + 1), 10, || false)
                .is_err()
        );
        assert!(
            catalog
                .search_checked(&"common ".repeat(33), 10, || false)
                .is_err()
        );
        assert_eq!(
            catalog.search("common", 10),
            catalog.search(&"common ".repeat(32), 10)
        );
        let mut checks = 0;
        let results = catalog
            .search_checked("alpha beta", usize::MAX, || {
                checks += 1;
                false
            })
            .unwrap();
        assert_eq!(checks, 20000);
        assert_eq!(results.len(), 50);
        for stop in [3, 10003] {
            let mut checks = 0;
            assert!(
                catalog
                    .search_checked("common", 10, || {
                        checks += 1;
                        checks == stop
                    })
                    .is_err()
            );
            assert_eq!(checks, stop);
        }
    }

    #[test]
    fn declarations_respect_every_small_budget() {
        let catalog = Catalog::new([entry("example", "description")]).unwrap();
        for budget in 0..256 {
            assert!(catalog.render_declarations(budget).len() <= budget);
        }
    }

    #[test]
    fn rejects_duplicates_and_invalid_names() {
        let error = Catalog::new([entry("a", ""), entry("a", "")]).unwrap_err();
        assert_eq!(error, CatalogError::DuplicateName("a".into()));
        assert!(matches!(
            Catalog::new([entry("", "")]).unwrap_err(),
            CatalogError::InvalidName { .. }
        ));
        assert!(matches!(
            Catalog::new([entry("has space", "")]).unwrap_err(),
            CatalogError::InvalidName { .. }
        ));
        assert!(Catalog::new([entry(&"x".repeat(129), "")]).is_err());
    }

    #[test]
    fn exact_names_do_not_alias() {
        let catalog = Catalog::new([entry("a-b", "dash"), entry("a_b", "underscore")]).unwrap();
        assert_eq!(catalog.get("a-b").unwrap().description(), "dash");
        assert_eq!(catalog.get("a_b").unwrap().description(), "underscore");
        assert!(catalog.get("a.b").is_none());
        assert!(catalog.get("__proto__").is_none());
        assert!(catalog.get("constructor").is_none());
    }

    #[test]
    fn search_ranks_name_hits_first_and_breaks_ties_by_name() {
        let catalog = Catalog::new([
            entry("deployments.list", "List deployments for an environment"),
            entry(
                "issues.search",
                "Search issues by text, including deployments",
            ),
            entry("zeta", "deployments deployments"),
            entry("alpha", "deployments deployments"),
        ])
        .unwrap();
        let results: Vec<&str> = catalog
            .search("deployments", 10)
            .iter()
            .map(|e| e.name())
            .collect();
        assert_eq!(results[0], "deployments.list");
        let alpha = results.iter().position(|n| *n == "alpha").unwrap();
        let zeta = results.iter().position(|n| *n == "zeta").unwrap();
        assert!(alpha < zeta, "equal scores tie-break on name: {results:?}");
        assert!(catalog.search("nonexistent", 10).is_empty());
        assert_eq!(catalog.search("deployments", 1).len(), 1);
    }

    #[test]
    fn renders_declarations_within_budget_and_counts_hidden_entries() {
        let catalog = Catalog::new([
            CatalogEntry::new(
                "issues.search",
                "Search issues.",
                json!({"type": "object", "properties": {"query": {"type": "string"}, "limit": {"type": "integer"}}, "required": ["query"]}),
            )
            .with_output_schema(json!({"type": "object", "properties": {"items": {"type": "array", "items": {"type": "string"}}}})),
            entry("hidden", "deferred").with_presentation(Presentation::Deferred),
            entry("big", &"x".repeat(400)),
        ])
        .unwrap();
        let rendered = catalog.render_declarations(400);
        assert!(rendered.contains(
            "\"issues.search\"(args: { limit?: number; query: string }): Promise<{ items?: string[] }>;"
        ), "{rendered}");
        assert!(!rendered.contains("hidden"));
        assert!(!rendered.contains("\"big\""));
        assert!(rendered.contains("2 more tool(s)"));
    }

    #[test]
    fn unsupported_schema_renders_unknown_not_invented_types() {
        assert_eq!(render_type(&json!({"$ref": "#/defs/x"}), 0), "unknown");
        assert_eq!(
            render_type(&json!({"type": "string", "format": "uri"}), 0),
            "string"
        );
        assert_eq!(render_type(&json!({"enum": ["a", 1]}), 0), "\"a\" | 1");
        assert_eq!(
            render_type(&json!({"type": ["string", "null"]}), 0),
            "string | null"
        );
        assert_eq!(
            render_type(
                &json!({"anyOf": [{"type": "string"}, {"type": "number"}]}),
                0
            ),
            "string | number"
        );
        assert_eq!(
            render_type(
                &json!({"type": "object", "additionalProperties": {"type": "number"}}),
                0
            ),
            "Record<string, number>"
        );
        let mut deep = json!({"type": "string"});
        for _ in 0..10 {
            deep = json!({"type": "array", "items": deep});
        }
        assert!(render_type(&deep, 0).contains("unknown"));
    }
}
