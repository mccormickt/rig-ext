//! Explicit host mode with mock tools: the host owns the dispatcher and its
//! policies, reviews each script once before it runs, and a script composes
//! several tool calls into one summary.
//!
//! ```sh
//! cargo run -p rig-codemode --example explicit_host --features quickjs
//! ```

use std::sync::Arc;

use rig_codemode::{
    Catalog, CatalogEntry, CodeMode, DynamicToolDispatcher, ExecutionRequest, Invocation, Limits,
    ScriptReview,
};
use rig_core::tool::{DynamicTool, ToolExecutionError, ToolOutput, ToolResult};
use serde_json::{Value, json};

fn mock_tools() -> Vec<DynamicTool> {
    vec![
        DynamicTool::new(
            "list_orders",
            "List orders for a customer. Returns an array of { id, total, status }.",
            json!({
                "type": "object",
                "properties": { "customer": { "type": "string" } },
                "required": ["customer"]
            }),
            |args| {
                Box::pin(async move {
                    let customer = args["customer"].as_str().unwrap_or("unknown");
                    Ok(ToolOutput::json(json!([
                        { "id": "o-1", "customer": customer, "total": 120.5, "status": "shipped" },
                        { "id": "o-2", "customer": customer, "total": 42.0, "status": "pending" },
                        { "id": "o-3", "customer": customer, "total": 9.99, "status": "pending" },
                    ])))
                })
            },
        ),
        DynamicTool::new(
            "order_details",
            "Details for one order, including the shipping address.",
            json!({
                "type": "object",
                "properties": { "id": { "type": "string" } },
                "required": ["id"]
            }),
            |args| {
                Box::pin(async move {
                    let id = args["id"].as_str().unwrap_or_default();
                    Ok(ToolOutput::json(json!({
                        "id": id,
                        "items": [{ "sku": format!("sku-{id}"), "qty": 2 }],
                        "address": "1 Example Street",
                        "card_last4": "4242",
                    })))
                })
            },
        ),
        DynamicTool::new(
            "cancel_order",
            "Cancel a pending order.",
            json!({
                "type": "object",
                "properties": { "id": { "type": "string" } },
                "required": ["id"]
            }),
            |args| {
                Box::pin(async move {
                    Ok(ToolOutput::text(format!(
                        "cancelled {}",
                        args["id"].as_str().unwrap_or_default()
                    )))
                })
            },
        ),
    ]
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dispatcher = DynamicToolDispatcher::new(mock_tools())?
        // Call policy: deny writes on behalf of this parent call.
        .with_call_policy(|invocation: &mut Invocation| {
            if invocation.name == "cancel_order" {
                return Err(ToolExecutionError::refused(
                    "cancel_order is read-only in this session",
                ));
            }
            Ok(())
        })
        // Result policy: strip payment data before anything reaches the script.
        .with_result_policy(|_invocation: &Invocation, result: ToolResult| {
            let Some(mut value) = result.output().as_json().cloned() else {
                return result;
            };
            if let Value::Object(object) = &mut value {
                object.remove("card_last4");
            }
            result.with_output(ToolOutput::json(value))
        });

    let catalog = Catalog::new(
        dispatcher
            .definitions()
            .iter()
            .map(CatalogEntry::from_definition),
    )?;

    let codemode = CodeMode::builder(catalog, Arc::new(dispatcher))
        .limits(Limits {
            max_calls: 16,
            max_in_flight: 4,
            ..Limits::default()
        })
        // Script policy: review the whole script once. Grant only the tools
        // it names; a computed `tools[name]` outside that set is refused.
        .script_policy(|review: ScriptReview<'_>| {
            println!(
                "== script review ==\n  names {:?}, dynamic access {}",
                review.analysis.tools, review.analysis.dynamic_tool_access
            );
            Ok(review.grant_referenced())
        })
        .build()?;

    println!(
        "== tool description shown to the model ==\n{}",
        codemode.description()
    );

    let script = r#"
        const orders = await tools.list_orders({ customer: "acme" });
        const pending = orders.filter(o => o.status === "pending");
        const details = await Promise.all(pending.map(o => tools.order_details({ id: o.id })));
        text(`${pending.length} pending orders, total ${pending.reduce((s, o) => s + o.total, 0)}`);
        for (const d of details) text({ id: d.id, address: d.address, hasCard: "card_last4" in d });
        try {
            await tools.cancel_order({ id: pending[0].id });
        } catch (e) {
            text(`cancel refused: ${e.message} (status=${e.status}, kind=${e.kind})`);
        }
        return pending.map(o => o.id);
    "#;

    let report = codemode.execute(ExecutionRequest::new(script)).await?;
    println!("== model-visible result ==\n{}", report.render_text());
    println!("== host record ==");
    for call in &report.calls {
        println!(
            "  #{} {:<14} {:?} ({:?})",
            call.ordinal, call.name, call.status, call.delivery
        );
    }
    println!("  status {:?} in {:?}", report.status, report.elapsed);
    Ok(())
}
