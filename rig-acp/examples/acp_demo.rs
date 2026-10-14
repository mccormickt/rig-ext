use futures::StreamExt;
use rig_acp::{
    Client, Event, Session, Turn,
    protocol::{AcpAgent, AcpAgentConfig},
    schema::*,
};
use rig_agent::{
    agent::AgentBuilder,
    test_utils::{MockCompletionModel, MockTurn},
};
use rig_core::{
    completion::CompletionRequest,
    streaming::{Item, StreamEvent},
};

async fn collect(mut turn: Turn, allow: bool) -> anyhow::Result<(String, StopReason)> {
    let mut text = String::new();
    let mut stop = None;
    while let Some(event) = turn.next().await {
        match event? {
            Event::Update(notification) => {
                println!(
                    "update session={} {}",
                    notification.session_id,
                    serde_json::to_string(&notification.update)?
                );
                if let SessionUpdate::AgentMessageChunk(chunk) = notification.update
                    && let ContentBlock::Text(chunk) = chunk.content
                {
                    text.push_str(&chunk.text);
                }
            }
            Event::Permission(permission) => {
                let kind = if allow {
                    PermissionOptionKind::AllowOnce
                } else {
                    PermissionOptionKind::RejectOnce
                };
                let option = permission
                    .request()
                    .options
                    .iter()
                    .find(|o| o.kind == kind)
                    .ok_or_else(|| anyhow::anyhow!("missing offered option"))?
                    .option_id
                    .clone();
                println!(
                    "permission session={} tool={} selected={option}",
                    permission.request().session_id,
                    permission.request().tool_call.tool_call_id
                );
                permission.respond(RequestPermissionOutcome::Selected(
                    SelectedPermissionOutcome::new(option),
                ))?;
            }
            Event::Completed(response) => {
                anyhow::ensure!(stop.is_none(), "duplicate completion");
                println!(
                    "complete session={} stop={:?}",
                    turn.session().id(),
                    response.stop_reason
                );
                stop = Some(response.stop_reason);
            }
        }
    }
    Ok((
        text,
        stop.ok_or_else(|| anyhow::anyhow!("missing completion"))?,
    ))
}

async fn text(session: &Session, prompt: &str) -> anyhow::Result<String> {
    let (text, stop) = collect(session.prompt_text(prompt)?, false).await?;
    anyhow::ensure!(stop == StopReason::EndTurn);
    Ok(text)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tokio::time::timeout(std::time::Duration::from_secs(30), demo()).await?
}

async fn demo() -> anyhow::Result<()> {
    let client = if let Ok(endpoint) = std::env::var("ACP_RIVET_URL") {
        #[cfg(feature = "rivet")]
        {
            let mut headers = reqwest::header::HeaderMap::new();
            if let Ok(token) = std::env::var("ACP_RIVET_TOKEN") {
                let mut value = reqwest::header::HeaderValue::from_str(&format!("Bearer {token}"))?;
                value.set_sensitive(true);
                headers.insert(reqwest::header::AUTHORIZATION, value);
            }
            println!("transport=Rivet HTTP/SSE; real server, scripted agent");
            Client::connect(rig_acp::rivet::RivetTransport::new(
                reqwest::Client::builder()
                    .default_headers(headers)
                    .retry(reqwest::retry::never())
                    .redirect(reqwest::redirect::Policy::none())
                    .build()?,
                endpoint.parse()?,
            ))
            .await?
        }
        #[cfg(not(feature = "rivet"))]
        {
            let _ = endpoint;
            anyhow::bail!("enable the rivet feature");
        }
    } else {
        println!("transport=official SDK process/stdio; independent Node fixture, no LLM");
        let config = AcpAgentConfig::new("node").arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/examples/fixtures/agent.mjs"
        ));
        Client::connect(AcpAgent::new(config)).await?
    };
    println!(
        "initialized protocol={:?} agent={:?}",
        client.initialized().protocol_version,
        client.initialized().agent_info
    );
    let session = client.new_session(std::env::current_dir()?).await?;
    let isolated = client.new_session(std::env::current_dir()?).await?;
    anyhow::ensure!(text(&session, "remember amber-73").await? == "remembered");
    anyhow::ensure!(text(&session, "recall").await? == "amber-73");
    anyhow::ensure!(text(&isolated, "recall").await? == "empty");
    for (allow, expected) in [
        (true, "before:during:approved"),
        (false, "before:during:denied"),
    ] {
        let (reply, stop) = collect(session.prompt_text("permission")?, allow).await?;
        anyhow::ensure!(reply == expected && stop == StopReason::EndTurn);
    }
    let mut waiting = session.prompt_text("wait")?;
    let ready = waiting
        .next()
        .await
        .transpose()?
        .ok_or_else(|| anyhow::anyhow!("missing gate"))?;
    anyhow::ensure!(matches!(ready, Event::Update(_)));
    session.cancel()?;
    let (_, stop) = collect(waiting, false).await?;
    anyhow::ensure!(stop == StopReason::Cancelled);
    anyhow::ensure!(text(&isolated, "release").await? == "released");
    let stats: serde_json::Value = serde_json::from_str(&text(&session, "stats").await?)?;
    anyhow::ensure!(stats["effects"] == 1 && stats["cancels"] == 1);
    println!(
        "EFFECT RECEIPT session={} {stats}; approval=1 denial=0 cancellation=0",
        session.id()
    );

    let model = session.model();
    let mut stream = model.stream(CompletionRequest::new("stream-λ"))?;
    let mut chunks = Vec::new();
    while let Some(item) = stream.next().await {
        if let Item::Event(StreamEvent::Text { text, .. }) = item? {
            chunks.push(text);
        }
    }
    anyhow::ensure!(chunks == ["ec", "ho:stream-λ"]);
    println!("Rig streamed exact chunks: {chunks:?}");
    let child = model.agent("demo-local-memory").build();
    anyhow::ensure!(child.prompt("recall").await?.to_string() == "amber-73");
    let parent_model = MockCompletionModel::from_turns([
        MockTurn::tool_call(
            "delegation-1",
            "acp_delegate",
            serde_json::json!({"prompt":"recall"}),
        ),
        MockTurn::text("parent finished"),
    ]);
    let parent = AgentBuilder::new(parent_model.clone())
        .dynamic_tool(child.into_tool()?)
        .build();
    anyhow::ensure!(parent.prompt("delegate").max_turns(3).await?.to_string() == "parent finished");
    let recorded = serde_json::to_string(&parent_model.requests())?;
    anyhow::ensure!(
        recorded.contains("amber-73"),
        "child result must reach parent model"
    );
    println!("Rig sub-agent receipt: actual ACP recall returned amber-73 to parent model");
    println!(
        "PASS: continuation, isolation, ordered streaming, approval, denial, cancelled effect, next turn, Rig agent and sub-agent"
    );
    Ok(())
}
