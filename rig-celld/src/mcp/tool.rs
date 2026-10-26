use std::sync::Arc;

use rig_core::{
    message::{ToolName, ToolResultContent},
    tool::{ContextValue, DynamicTool, ToolExecutionError, ToolOutput},
};
use rmcp::model::{CallToolResult, ContentBlock, Tool};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{Client, Error, Transport, headers};

/// Host-supplied options. Insert into Rig's inbound `ToolContext` for dynamic
/// calls. Only use an idempotency key when the endpoint defines and enforces it.
/// The client does not infer keys from JSON-RPC IDs or tool annotations.
#[derive(Default, Clone, Serialize, Deserialize)]
pub struct CallOptions {
    pub idempotency_key: Option<String>,
}

impl ContextValue for CallOptions {
    const KEY: &'static str = "rig_mcp.call_options";
}

struct Definition {
    wire: Tool,
    name: ToolName,
    input: jsonschema::Validator,
    output: Option<jsonschema::Validator>,
    headers: Vec<headers::ParameterHeader>,
}

/// A retained tool definition and its compiled validation rules.
pub struct McpTool<T> {
    client: Client<T>,
    definition: Arc<Definition>,
}

impl<T> Clone for McpTool<T> {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            definition: self.definition.clone(),
        }
    }
}

impl<T: Transport> McpTool<T> {
    pub(crate) fn new(client: Client<T>, wire: Tool) -> Result<Self, Error> {
        super::client::encode(&wire, client.limits.catalog_bytes)?;
        let name = ToolName::new(wire.name.to_string()).map_err(|_| Error::Schema)?;
        let input = Value::Object(wire.input_schema.as_ref().clone());
        let headers = headers::compile(&input)?;
        let input = jsonschema::options()
            .offline()
            .build(&input)
            .map_err(|_| Error::Schema)?;
        let output = wire
            .output_schema
            .as_ref()
            .map(|schema| {
                jsonschema::options()
                    .offline()
                    .build(&Value::Object(schema.as_ref().clone()))
                    .map_err(|_| Error::Schema)
            })
            .transpose()?;
        Ok(Self {
            client,
            definition: Arc::new(Definition {
                wire,
                name,
                input,
                output,
                headers,
            }),
        })
    }

    /// Preserve this definition before conversion: Rig definitions do not retain
    /// MCP output schemas. Treat descriptions and annotations as untrusted data.
    pub fn definition(&self) -> &Tool {
        &self.definition.wire
    }

    /// Make exactly one tool request after input validation. `isError` remains
    /// available to direct callers; successful structured results are validated.
    /// Raw response metadata is returned only to this host-facing API.
    pub async fn call(
        &self,
        arguments: Value,
        options: &CallOptions,
    ) -> Result<CallToolResult, Error> {
        if !arguments.is_object() || !self.definition.input.is_valid(&arguments) {
            return Err(Error::Arguments);
        }
        let mut headers = headers::extract(&self.definition.headers, &arguments)?;
        headers.push((
            "Mcp-Name".into(),
            headers::encoded(&self.definition.wire.name),
        ));
        if let Some(key) = &options.idempotency_key {
            if key.is_empty()
                || key.len() > 512
                || !key.bytes().all(|byte| (0x21..=0x7e).contains(&byte))
            {
                return Err(Error::Configuration("invalid idempotency key"));
            }
            headers.push(("Idempotency-Key".into(), key.clone()));
        }
        let mut params = serde_json::Map::new();
        params.insert(
            "name".into(),
            Value::String(self.definition.wire.name.to_string()),
        );
        params.insert("arguments".into(), arguments);
        let (value, _) = self
            .client
            .rpc(
                "tools/call",
                params,
                headers,
                self.client.limits.response_bytes,
            )
            .await?;
        let result: CallToolResult = serde_json::from_value(value)
            .map_err(|_| Error::Protocol("invalid tools/call result"))?;
        if result.is_error != Some(true)
            && let Some(validator) = &self.definition.output
            && !result
                .structured_content
                .as_ref()
                .is_some_and(|value| validator.is_valid(value))
        {
            return Err(Error::OutputSchema);
        }
        Ok(result)
    }

    /// Convert to a Rig tool for ordinary agents or code-mode dispatch. Results
    /// expose content and structured content, never MCP response/block metadata.
    /// Errors are nonretryable; durable hosts must separately declare replay safety.
    pub fn into_dynamic_tool(self) -> DynamicTool {
        DynamicTool::new_with_context(
            self.definition.name.clone(),
            self.definition
                .wire
                .description
                .as_deref()
                .unwrap_or("")
                .to_string(),
            Value::Object(self.definition.wire.input_schema.as_ref().clone()),
            move |context, arguments| {
                let tool = self.clone();
                Box::pin(async move {
                    let options = context
                        .get::<CallOptions>()
                        .map_err(|_| tool_error(Error::Configuration("invalid call options")))?
                        .unwrap_or_default();
                    let result = tool.call(arguments, &options).await.map_err(tool_error)?;
                    let output = output(&result)?;
                    if result.is_error == Some(true) {
                        Err(ToolExecutionError::other("MCP tool reported an error")
                            .with_retryable(false)
                            .with_model_output(output))
                    } else {
                        Ok(output)
                    }
                })
            },
        )
    }
}

fn tool_error(error: Error) -> ToolExecutionError {
    let error = match error {
        Error::Arguments => ToolExecutionError::invalid_args(error.to_string()),
        Error::Timeout => ToolExecutionError::timeout(error.to_string()),
        Error::Transport => ToolExecutionError::network(error.to_string()),
        _ => ToolExecutionError::other(error.to_string()),
    };
    error.with_retryable(false)
}

fn output(result: &CallToolResult) -> Result<ToolOutput, ToolExecutionError> {
    let mut blocks = Vec::new();
    let mut structured = result.structured_content.clone();
    for block in &result.content {
        if let ContentBlock::Text(text) = block {
            if structured.as_ref().is_some_and(|value| {
                serde_json::from_str::<Value>(&text.text).is_ok_and(|parsed| parsed == *value)
            }) {
                if let Some(value) = structured.take() {
                    blocks.push(ToolResultContent::json(value));
                }
            } else {
                blocks.push(ToolResultContent::text(&text.text));
            }
        } else {
            // Serialize only public content fields; metadata is not model output.
            let mut value = serde_json::to_value(block)
                .map_err(|_| tool_error(Error::Protocol("invalid content")))?;
            if let Some(object) = value.as_object_mut() {
                object.remove("_meta");
                object.remove("annotations");
                if let Some(resource) = object.get_mut("resource").and_then(Value::as_object_mut) {
                    resource.remove("_meta");
                    resource.remove("annotations");
                }
            }
            match block {
                ContentBlock::Image(image) => {
                    use rig_core::message::{ImageMediaType, MimeType};
                    if let Some(media_type) = ImageMediaType::from_mime_type(&image.mime_type) {
                        blocks.push(ToolResultContent::image_base64(
                            &image.data,
                            Some(media_type),
                            None,
                        ));
                    } else {
                        blocks.push(ToolResultContent::json(value));
                    }
                }
                ContentBlock::Audio(_)
                | ContentBlock::Resource(_)
                | ContentBlock::ResourceLink(_) => {
                    blocks.push(ToolResultContent::json(value));
                }
                _ => return Err(tool_error(Error::Unsupported("content block"))),
            }
        }
    }
    if let Some(value) = structured {
        blocks.insert(0, ToolResultContent::json(value));
    }
    if blocks.is_empty() {
        Ok(ToolOutput::text(if result.is_error == Some(true) {
            "MCP tool reported an error"
        } else {
            ""
        }))
    } else {
        ToolOutput::content(blocks)
    }
}
