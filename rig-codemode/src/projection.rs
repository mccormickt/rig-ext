//! Safe, bounded projection of tool results into script values.

use rig_core::completion::message::ToolResultContent;
use rig_core::tool::{ToolErrorKind, ToolOutput, ToolResult};
use serde::Serialize;

use crate::bridge::{Payload, ScriptError};
use crate::report::ScriptDelivery;

#[derive(Serialize)]
struct RawEnvelope<'a> {
    status: &'static str,
    name: &'a str,
    content: &'a [ToolResultContent],
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<RawError<'a>>,
}

#[derive(Serialize)]
struct RawError<'a> {
    kind: ToolErrorKind,
    message: String,
    retryable: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    code: Option<&'a str>,
}

/// JSON writer with byte and nesting ceilings. It fails before copying a
/// write that exceeds either ceiling; serde then stops visiting the value.
pub(crate) fn bounded_json(
    value: &(impl Serialize + ?Sized),
    limit: usize,
) -> Result<String, serde_json::Error> {
    bounded_json_with_depth(value, limit, 64)
}

pub(crate) fn bounded_json_with_depth(
    value: &(impl Serialize + ?Sized),
    limit: usize,
    max_depth: usize,
) -> Result<String, serde_json::Error> {
    struct Writer {
        bytes: Vec<u8>,
        limit: usize,
        depth: usize,
        max_depth: usize,
        quoted: bool,
        escaped: bool,
    }
    impl std::io::Write for Writer {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if bytes.len() > self.limit.saturating_sub(self.bytes.len()) {
                return Err(std::io::Error::other("JSON byte limit exceeded"));
            }
            for &byte in bytes {
                if self.quoted {
                    if self.escaped {
                        self.escaped = false;
                    } else if byte == b'\\' {
                        self.escaped = true;
                    } else if byte == b'"' {
                        self.quoted = false;
                    }
                } else {
                    match byte {
                        b'"' => self.quoted = true,
                        b'[' | b'{' => {
                            self.depth += 1;
                            if self.depth > self.max_depth {
                                return Err(std::io::Error::other("JSON nesting limit exceeded"));
                            }
                        }
                        b']' | b'}' => self.depth = self.depth.saturating_sub(1),
                        _ => {}
                    }
                }
            }
            self.bytes.extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut writer = Writer {
        bytes: Vec::new(),
        limit,
        depth: 0,
        max_depth,
        quoted: false,
        escaped: false,
    };
    serde_json::to_writer(&mut writer, value)?;
    // serde_json emits UTF-8, including when it escapes a string.
    String::from_utf8(writer.bytes)
        .map_err(|error| serde_json::Error::io(std::io::Error::other(error)))
}

fn bounded_render(output: &ToolOutput, limit: usize) -> Result<String, ()> {
    if let Some(text) = output.as_text() {
        return (text.len() <= limit).then(|| text.to_owned()).ok_or(());
    }
    if let Some(value) = output.as_json() {
        bounded_json(value, limit).map_err(|_| ())
    } else {
        bounded_json(output.as_content(), limit).map_err(|_| ())
    }
}

/// Project a policy-filtered result into what the script receives.
///
/// Normal mode: one JSON block is that value; one plain text block is the
/// literal string; mixed content is `{ "content": [...] }`; error, refusal,
/// and skip reject with a typed error carrying only model-visible text.
/// Raw mode: a status envelope for every disposition.
pub(crate) fn project(name: &str, result: &ToolResult, raw: bool, limit: usize) -> Payload {
    let unserializable = || {
        Payload::Reject(ScriptError::size(
            "tool result exceeds the byte or nesting limit",
            Some(name.into()),
        ))
    };
    if raw {
        let error = result.error().or_else(|| result.refusal());
        let message = match error
            .map(|error| bounded_render(error.model_output(), limit))
            .transpose()
        {
            Ok(message) => message,
            Err(()) => return unserializable(),
        };
        let envelope = RawEnvelope {
            status: result.status_name(),
            name,
            content: result.output().as_content(),
            error: error.map(|error| RawError {
                kind: error.kind(),
                message: message.unwrap_or_default(),
                retryable: error
                    .retryable()
                    .or_else(|| error.kind().default_retryable())
                    .unwrap_or(false),
                code: error.code(),
            }),
        };
        return bounded_json(&envelope, limit)
            .map(Payload::Resolve)
            .unwrap_or_else(|_| unserializable());
    }
    if result.is_success() {
        let output = result.output();
        let json = if let Some(value) = output.as_json() {
            bounded_json(value, limit)
        } else if let Some(text) = output.as_text() {
            bounded_json(text, limit)
        } else {
            #[derive(Serialize)]
            struct Content<'a> {
                content: &'a [ToolResultContent],
            }
            bounded_json(
                &Content {
                    content: output.as_content(),
                },
                limit,
            )
        };
        return json
            .map(Payload::Resolve)
            .unwrap_or_else(|_| unserializable());
    }
    let (status, kind) = if let Some(error) = result.refusal() {
        ("denied", error.kind())
    } else if let Some(error) = result.error() {
        ("error", error.kind())
    } else {
        ("skipped", ToolErrorKind::Cancelled)
    };
    Payload::Reject(ScriptError {
        message: match bounded_render(result.output(), limit) {
            Ok(message) => message,
            Err(()) => return unserializable(),
        },
        kind,
        status,
        tool: Some(name.to_string()),
    })
}

/// Enforce the message limit on a projected payload. Oversized results become
/// a size error so the guest never parses or copies them.
pub(crate) fn bound_payload(
    name: &str,
    payload: Payload,
    message_bytes: usize,
) -> (Payload, ScriptDelivery) {
    if matches!(&payload, Payload::Reject(error) if error.status == "size") {
        return (payload, ScriptDelivery::Oversized);
    }
    let size = match &payload {
        Payload::Resolve(json) => json.len(),
        Payload::Reject(error) => error.message.len(),
    };
    if size > message_bytes {
        let error = ScriptError::size(
            format!("tool result is {size} bytes; the limit is {message_bytes} bytes"),
            Some(name.to_string()),
        );
        (Payload::Reject(error), ScriptDelivery::Oversized)
    } else {
        (payload, ScriptDelivery::Delivered)
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]
mod tests {
    use super::*;
    use rig_core::tool::{ToolExecutionError, ToolOutput};
    use serde_json::json;

    fn project(name: &str, result: &ToolResult, raw: bool) -> Payload {
        super::project(name, result, raw, 1024)
    }

    #[test]
    fn bounded_writer_stops_visiting_and_limits_depth() {
        use serde::ser::SerializeSeq;
        struct Many<'a>(&'a std::cell::Cell<usize>);
        impl Serialize for Many<'_> {
            fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                let mut seq = serializer.serialize_seq(Some(1_000_000))?;
                for _ in 0..1_000_000 {
                    self.0.set(self.0.get() + 1);
                    seq.serialize_element("abcdefgh")?;
                }
                seq.end()
            }
        }
        let visits = std::cell::Cell::new(0);
        assert!(bounded_json(&Many(&visits), 24).is_err());
        assert_eq!(visits.get(), 3);
        assert_eq!(bounded_json("é", 4).unwrap(), "\"é\"");
        assert!(bounded_json("é", 3).is_err());
        let mut nested = json!(0);
        for _ in 0..64 {
            nested = json!([nested]);
        }
        assert_eq!(
            bounded_json(&nested, 1024).unwrap(),
            format!("{}0{}", "[".repeat(64), "]".repeat(64))
        );
        nested = json!([nested]);
        assert!(bounded_json(&nested, 1024).is_err());
        let escaped = json!({"x": "[\\\"{}]".repeat(80)});
        assert_eq!(bounded_json(&escaped, 1024).unwrap(), escaped.to_string());
        for raw in [false, true] {
            let result = ToolResult::failed(ToolExecutionError::other("x".repeat(2048)));
            assert!(
                matches!(project("t", &result, raw), Payload::Reject(error) if error.status == "size")
            );
        }
    }

    #[test]
    fn json_string_stays_structured_and_text_stays_literal() {
        let json = ToolResult::success(ToolOutput::json(json!("{\"x\":1}")));
        let text = ToolResult::success(ToolOutput::text("{\"x\":1}"));
        match (project("t", &json, false), project("t", &text, false)) {
            (Payload::Resolve(a), Payload::Resolve(b)) => {
                assert_eq!(a, "\"{\\\"x\\\":1}\"");
                assert_eq!(b, "\"{\\\"x\\\":1}\"");
            }
            other => panic!("unexpected {other:?}"),
        }
        let object = ToolResult::success(ToolOutput::json(json!({"x": 1})));
        assert!(matches!(project("t", &object, false), Payload::Resolve(s) if s == "{\"x\":1}"));
    }

    #[test]
    fn mixed_content_uses_envelope_in_order() {
        let output = ToolOutput::content(vec![
            ToolResultContent::text("a"),
            ToolResultContent::json(json!(1)),
        ])
        .unwrap();
        let Payload::Resolve(json) = project("t", &ToolResult::success(output), false) else {
            panic!("expected resolve");
        };
        let value: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(value["content"][0]["type"], "text");
        assert_eq!(value["content"][1]["value"], 1);
    }

    #[test]
    fn errors_reject_with_model_output_only() {
        let error = ToolExecutionError::other("operator detail: db password is hunter2")
            .with_model_feedback("the tool failed");
        let result = ToolResult::failed(error);
        let Payload::Reject(script_error) = project("t", &result, false) else {
            panic!("expected reject");
        };
        assert_eq!(script_error.message, "the tool failed");
        assert_eq!(script_error.status, "error");
        assert_eq!(script_error.kind, ToolErrorKind::Other);

        let Payload::Resolve(raw) = project("t", &result, true) else {
            panic!("expected envelope");
        };
        assert!(!raw.contains("hunter2"), "{raw}");
        let raw: serde_json::Value = serde_json::from_str(&raw).unwrap();
        assert_eq!(raw["status"], "error");
        assert_eq!(raw["error"]["message"], "the tool failed");
        assert_eq!(raw["error"]["retryable"], false);
    }

    #[test]
    fn refusal_and_skip_reject() {
        let refused = ToolResult::failed(ToolExecutionError::refused("no"));
        let Payload::Reject(error) = project("t", &refused, false) else {
            panic!()
        };
        assert_eq!(error.status, "denied");
        assert_eq!(error.kind, ToolErrorKind::PermissionDenied);
        let skipped = ToolResult::skipped("policy skipped");
        let Payload::Reject(error) = project("t", &skipped, false) else {
            panic!()
        };
        assert_eq!(error.status, "skipped");
        let Payload::Resolve(raw) = project("t", &skipped, true) else {
            panic!()
        };
        assert!(raw.contains("\"status\":\"skipped\""));
    }

    #[test]
    fn oversized_payload_becomes_size_error() {
        let payload = Payload::Resolve("x".repeat(11));
        let (payload, delivery) = bound_payload("t", payload, 10);
        assert_eq!(delivery, ScriptDelivery::Oversized);
        assert!(matches!(payload, Payload::Reject(e) if e.status == "size"));
        let (_, delivery) = bound_payload("t", Payload::Resolve("x".repeat(10)), 10);
        assert_eq!(delivery, ScriptDelivery::Delivered);
    }
}
