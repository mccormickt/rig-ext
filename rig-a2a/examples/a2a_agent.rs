//! Back a Rig agent with a remote A2A agent instead of an LLM provider.
//!
//! [`A2AModel`](rig_a2a::A2AModel) implements Rig's model wire over A2A's
//! `message/send` and `message/stream`, so `A2AClient::agent` returns an
//! ordinary Rig [`Agent`]: it prompts, it streams, and it composes as a
//! sub-agent tool on an orchestrator.
//!
//! What it cannot do is carry a chat-completion API's whole surface. A remote
//! A2A agent owns its instructions and hides its tools, so `tools` and
//! `output_schema` on a request fail loudly rather than being dropped —
//! shown at the end.
//!
//! ```sh
//! cargo run --example a2a_agent -p rig-a2a
//! ```
//!
//! [`Agent`]: rig_agent::agent::Agent

use futures::StreamExt;
use rig_a2a::{A2A, A2AClient, A2AConversationExt, A2AModelRouteExt, A2AOptions, A2AReply};
use rig_agent::agent::{AgentBuilder, AgentRun, AgentRunStep, ModelTurn, RetryRequest};
use rig_agent::test_utils::{MockCompletionModel, MockTurn};
use rig_core::streaming::{Item, StreamEvent};

#[path = "./fixtures/lib.rs"]
mod fixture;

#[derive(serde::Serialize, serde::Deserialize)]
struct SavedRemoteRun {
    endpoint: String,
    tenant: Option<String>,
    run: AgentRun,
    options: A2AOptions,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let stub = fixture::serve_stub_agent([
        fixture::Reply::completed("I index and answer questions about documents."),
        fixture::Reply::completed("Earlier you asked what I do."),
        fixture::Reply::completed("Streaming works the same way."),
        fixture::Reply::completed("Three documents mention the migration."),
        fixture::Reply::completed("The remote route answered."),
        fixture::Reply::input_required("Which document?"),
        fixture::Reply::completed("Updated README.md."),
        fixture::Reply::completed("Live protocol events retain their task state."),
    ])
    .await;
    let remote = A2AClient::from_url(&stub.base_url).await?;

    // The remote agent as a Rig agent. Its name and description come from the
    // card. Naming a conversation on the prompt threads it: one agent serves
    // every conversation, each with its own remote thread.
    let agent = remote.agent().build();
    println!("agent {:?}", agent.name());
    println!(
        "  turn 1: {}",
        agent
            .prompt("What do you do?")
            .a2a_conversation("user-42")
            .await?
    );
    println!(
        "  turn 2: {}\n",
        agent
            .prompt("What did I just ask?")
            .a2a_conversation("user-42")
            .await?
    );

    // The completion stream buffers replaceable artifacts until EOF. Use the
    // direct protocol stream below for live status and artifact events.
    let request = rig_core::completion::CompletionRequest::new("Stream me a reply.");
    let mut stream = remote.model().stream(request)?;
    print!("streamed: ");
    while let Some(chunk) = stream.next().await {
        if let Item::Event(StreamEvent::Text { text, .. }) = chunk? {
            print!("{text}");
        }
    }
    println!("\n");

    // Composition: an A2A-backed agent is an ordinary agent, so the sub-agent
    // bridge applies. Here a local orchestrator delegates to it.
    let researcher = remote.agent().a2a_conversation("research").build();
    let model = MockCompletionModel::from_turns([
        MockTurn::tool_call(
            "call_1",
            "librarian",
            serde_json::json!({"prompt": "Search for migration notes."}),
        ),
        MockTurn::text("Three documents mention the migration."),
    ]);
    let orchestrator = AgentBuilder::new(model)
        .preamble("Delegate research to the librarian sub-agent.")
        .dynamic_tool(researcher.into_tool()?)
        .build();
    println!(
        "orchestrator: {}\n",
        orchestrator
            .prompt("What do we know about the migration?")
            .max_turns(3)
            .await?
    );

    // The remote handler prepares history after application hooks.
    let routed = AgentBuilder::new(MockCompletionModel::from_turns([MockTurn::text("local")]))
        .a2a_model_route("remote", remote.model())
        .build();
    println!(
        "routed: {}",
        routed
            .prompt("Use the remote.")
            .using_model("remote")
            .await?
    );

    // Persist the remote IDs separately from the sans-IO AgentRun. No local
    // conversation cache is needed when resuming through explicit IDs.
    let mut run = AgentRun::new("Update a document.").max_turns(2);
    let AgentRunStep::CallModel { prompt, .. } = run.next_step()? else {
        anyhow::bail!("expected initial model call");
    };
    let response = remote
        .model()
        .completion(
            rig_core::completion::CompletionRequest::new(prompt)
                .accept_unknown_finish_reasons(true),
        )
        .await?;
    if let Some(Ok(A2AReply::Response(reply))) = response.extras::<A2A>()
        && let rig_a2a::SendMessageResponse::Task(task) = *reply
    {
        println!("remote state: {:?}", task.status.state);
        // Save the issued call's response before resuming, so the runner does
        // not repeat it. A host persists the response with its pending run.
        run.model_response(ModelTurn::from_policy(&response, Default::default()))?;
        run.retry_model_turn(RetryRequest::Feedback("README.md".into()))?;
        println!(
            "checkpoint: {} model turn recorded",
            run.completion_calls().len()
        );
        let saved = serde_json::to_vec(&SavedRemoteRun {
            endpoint: stub.base_url.clone(),
            tenant: remote.interface().tenant.clone(),
            run,
            options: A2AOptions::default().context(task.context_id).task(task.id),
        })?;
        let saved: SavedRemoteRun = serde_json::from_slice(&saved)?;
        let fresh_client = A2AClient::from_url(&saved.endpoint).await?;
        anyhow::ensure!(
            fresh_client.interface().tenant == saved.tenant,
            "remote tenant changed"
        );
        println!(
            "resumed: {}",
            fresh_client
                .agent()
                .build()
                .resume(saved.run)
                .provider_option(saved.options)
                .run()
                .await?
        );
    }

    let mut events = remote.message("Report progress.").stream().await?;
    while let Some(event) = events.next().await {
        println!("protocol event: {:?}", event?);
    }

    // A2A hides the remote's tools by design, so a remote agent never returns a
    // tool call. Registering local tools would advertise tools that can never
    // be invoked, so the request is refused instead.
    let mut with_tools = rig_core::completion::CompletionRequest::new("hi");
    with_tools
        .tools
        .push(std::sync::Arc::new(rig_core::completion::ToolDefinition {
            name: rig_core::message::ToolName::new("add")?,
            description: "adds two numbers".to_string(),
            parameters: serde_json::json!({}),
            annotations: Default::default(),
        }));
    match remote.model().completion(with_tools).await {
        Ok(_) => println!("unexpectedly accepted a tool-bearing request"),
        Err(error) => println!("refused, as intended: {error}"),
    }

    Ok(())
}
