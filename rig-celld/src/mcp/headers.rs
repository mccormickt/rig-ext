use std::collections::BTreeSet;

use base64::{Engine, engine::general_purpose::STANDARD};
use serde_json::Value;

use super::Error;

pub(crate) struct ParameterHeader {
    path: Vec<String>,
    name: String,
    kind: String,
}

pub(crate) fn encoded(value: &str) -> String {
    if value.trim() != value
        || !value.bytes().all(|b| (0x20..=0x7e).contains(&b))
        || (value.starts_with("=?base64?") && value.ends_with("?="))
    {
        format!("=?base64?{}?=", STANDARD.encode(value))
    } else {
        value.into()
    }
}

pub(crate) fn compile(schema: &Value) -> Result<Vec<ParameterHeader>, Error> {
    fn walk(
        value: &Value,
        path: Option<Vec<String>>,
        headers: &mut Vec<ParameterHeader>,
        names: &mut BTreeSet<String>,
    ) -> Result<(), Error> {
        let Some(object) = value.as_object() else {
            return Ok(());
        };
        if let Some(name) = object.get("x-mcp-header") {
            let path = path
                .as_ref()
                .filter(|p| !p.is_empty())
                .ok_or(Error::HeaderAnnotation)?;
            let name = name
                .as_str()
                .filter(|name| !name.is_empty())
                .ok_or(Error::HeaderAnnotation)?;
            if !name
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&b))
                || !names.insert(name.to_ascii_lowercase())
            {
                return Err(Error::HeaderAnnotation);
            }
            let kind = object
                .get("type")
                .and_then(Value::as_str)
                .ok_or(Error::HeaderAnnotation)?;
            if !matches!(kind, "string" | "integer" | "boolean") {
                return Err(Error::HeaderAnnotation);
            }
            headers.push(ParameterHeader {
                path: path.clone(),
                name: format!("Mcp-Param-{name}"),
                kind: kind.into(),
            });
        }
        for (key, child) in object {
            match key.as_str() {
                "properties" => {
                    if let Some(properties) = child.as_object() {
                        for (name, schema) in properties {
                            let next = path.as_ref().map(|path| {
                                let mut path = path.clone();
                                path.push(name.clone());
                                path
                            });
                            walk(schema, next, headers, names)?;
                        }
                    }
                }
                "$defs" | "definitions" | "patternProperties" | "dependentSchemas"
                | "dependencies" => {
                    if let Some(schemas) = child.as_object() {
                        for schema in schemas.values() {
                            walk(schema, None, headers, names)?;
                        }
                    }
                }
                "allOf" | "anyOf" | "oneOf" | "prefixItems" => {
                    if let Some(schemas) = child.as_array() {
                        for schema in schemas {
                            walk(schema, None, headers, names)?;
                        }
                    }
                }
                "items"
                | "not"
                | "if"
                | "then"
                | "else"
                | "contains"
                | "contentSchema"
                | "additionalProperties"
                | "unevaluatedProperties"
                | "unevaluatedItems"
                | "propertyNames"
                | "additionalItems" => {
                    if let Some(schemas) = child.as_array() {
                        for schema in schemas {
                            walk(schema, None, headers, names)?;
                        }
                    } else {
                        walk(child, None, headers, names)?;
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }
    let mut headers = Vec::new();
    walk(schema, Some(Vec::new()), &mut headers, &mut BTreeSet::new())?;
    Ok(headers)
}

pub(crate) fn extract(
    headers: &[ParameterHeader],
    arguments: &Value,
) -> Result<Vec<(String, String)>, Error> {
    let mut values = Vec::new();
    for header in headers {
        let value = header
            .path
            .iter()
            .try_fold(arguments, |value, key| value.get(key));
        let Some(value) = value.filter(|value| !value.is_null()) else {
            continue;
        };
        let text = match header.kind.as_str() {
            "string" => value.as_str().map(str::to_owned).ok_or(Error::Arguments)?,
            "boolean" => value
                .as_bool()
                .map(|value| value.to_string())
                .ok_or(Error::Arguments)?,
            "integer" => {
                let number = value.as_f64().ok_or(Error::Arguments)?;
                if !number.is_finite()
                    || number.fract() != 0.0
                    || number.abs() > 9_007_199_254_740_991.0
                {
                    return Err(Error::Arguments);
                }
                (number as i64).to_string()
            }
            _ => return Err(Error::HeaderAnnotation),
        };
        values.push((header.name.clone(), encoded(&text)));
    }
    Ok(values)
}
