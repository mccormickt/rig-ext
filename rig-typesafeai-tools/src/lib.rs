//! Expose a host-configured Jev evaluation model as a Rig tool.
//!
//! [`evaluation_tool`] accepts runtime questions and returns native answers,
//! model identity, and optional usage. The host owns authorization, credentials,
//! transport, deadlines, budgets, and result policy. Probabilities are judgments,
//! not permission grants.
//!
//! ```
//! use rig_core::tool::DynamicTool;
//! use rig_core::wasm_compat::{WasmCompatSend, WasmCompatSync};
//! use rig_typesafeai::Evaluate;
//! use rig_typesafeai_tools::evaluation_tool;
//!
//! fn tool(model: impl Evaluate + Clone + WasmCompatSend + WasmCompatSync + 'static)
//!     -> DynamicTool
//! {
//!     evaluation_tool(model)
//! }
//! ```

use std::{collections::BTreeMap, sync::LazyLock};

use rig_core::{
    message::ToolName,
    tool::{DynamicTool, ToolExecutionError, ToolOutput},
    wasm_compat::{WasmCompatSend, WasmCompatSync},
};
use rig_typesafeai::{DynamicQuery, Evaluate, types::Question};
use serde_json::{Value, json};

fn parameters() -> Value {
    json!({
        "type": "object",
        "required": ["state", "questions"],
        "additionalProperties": false,
        "properties": {
            "state": {"type": ["string", "object", "array"]},
            "questions": {
                "type": "object", "minProperties": 1,
                "propertyNames": {"minLength": 1},
                "additionalProperties": {"oneOf": [
                    {
                        "type": "object", "required": ["type", "instructions", "criteria"],
                        "additionalProperties": false,
                        "properties": {
                            "type": {"const": "choice"}, "instructions": {},
                            "criteria": {"type": "object", "minProperties": 2, "maxProperties": 255}
                        }
                    },
                    {
                        "type": "object", "required": ["type", "instructions", "criteria"],
                        "additionalProperties": false,
                        "properties": {
                            "type": {"const": "score"}, "instructions": {},
                            "criteria": {"type": "array", "minItems": 2, "maxItems": 10}
                        }
                    },
                    {
                        "type": "object", "required": ["type", "instructions"],
                        "additionalProperties": false,
                        "properties": {
                            "type": {"const": "noul"}, "instructions": {},
                            "criteria": {
                                "type": ["object", "null"], "required": ["true", "false"],
                                "additionalProperties": false,
                                "properties": {"true": {}, "false": {}}
                            }
                        }
                    }
                ]}
            }
        }
    })
}

#[expect(clippy::expect_used, reason = "the input schema is a library constant")]
static VALIDATOR: LazyLock<jsonschema::Validator> = LazyLock::new(|| {
    jsonschema::validator_for(&parameters()).expect("valid evaluation tool schema")
});

/// Build `jev_evaluate`, accepting `{state, questions}` and returning
/// `{answers, model, usage}`. `usage` is null when the provider did not report it.
/// Native choice/score/noul answers retain their distributions and confidence.
/// Provider request IDs, raw responses, and provider error details are not exposed.
///
/// The supplied model owns its endpoint, credentials, and transport. Invalid
/// arguments fail before evaluation, including on direct [`DynamicTool::execute`]
/// calls. Evaluation failures become non-retryable `other` tool errors with a
/// fixed message. No retry, timeout, quota, authorization, or result policy is
/// installed by this adapter; enforce these in the host and its transport.
/// Dropping a call does not prove that provider work stopped.
///
/// Native models must be `Send + Sync`; `wasm32-unknown-unknown` permits local
/// models and futures through Rig's portable bounds.
#[expect(clippy::expect_used, reason = "the tool name is a nonempty constant")]
pub fn evaluation_tool(
    model: impl Evaluate + Clone + WasmCompatSend + WasmCompatSync + 'static,
) -> DynamicTool {
    DynamicTool::new(
        ToolName::new("jev_evaluate").expect("valid evaluation tool name"),
        "Evaluate explicit state with TypeSafe Jev choice, score, or noul questions. \
         Returns {answers, model, usage}; answers retain native probabilities and confidence. \
         Use another call for questions that depend on an answer. Judgments do not grant permission.",
        parameters(),
        move |args| {
            let model = model.clone();
            Box::pin(async move {
                if !VALIDATOR.is_valid(&args) {
                    return Err(ToolExecutionError::invalid_args(
                        "Invalid Jev evaluation arguments",
                    ));
                }
                let definitions: BTreeMap<String, Question> =
                    serde_json::from_value(args["questions"].clone())
                        .map_err(|_| ToolExecutionError::invalid_args("Invalid Jev questions"))?;
                let query = DynamicQuery::new(definitions)
                    .map_err(|_| ToolExecutionError::invalid_args("Invalid Jev questions"))?;
                let result = model.evaluate(&args["state"], query).await.map_err(|_| {
                    ToolExecutionError::other("Jev evaluation failed or returned invalid answers")
                        .with_retryable(false)
                })?;
                Ok(ToolOutput::json(json!({
                    "answers": result.answers,
                    "model": result.model,
                    "usage": result.usage,
                })))
            })
        },
    )
}
