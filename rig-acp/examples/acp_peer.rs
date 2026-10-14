//! Run a configured ACP stdio peer. Arguments are an executable and its argv.
//! Without ACP_SMOKE_TEXT this probes only initialization and session creation.
use rig_acp::{
    Client,
    protocol::{AcpAgent, AcpAgentConfig},
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tokio::time::timeout(std::time::Duration::from_secs(90), run()).await?
}

async fn run() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let executable = args
        .next()
        .ok_or_else(|| anyhow::anyhow!("provide an ACP executable and argv"))?;
    let client = Client::connect(AcpAgent::new(AcpAgentConfig::new(executable).args(args))).await?;
    println!(
        "ACP initialized version={:?} agent={:?}",
        client.initialized().protocol_version,
        client.initialized().agent_info
    );
    let a = client.new_session(std::env::current_dir()?).await?;
    let b = client.new_session(std::env::current_dir()?).await?;
    println!("sessions A={} B={}", a.id(), b.id());
    if let Ok(prompt) = std::env::var("ACP_SMOKE_TEXT") {
        let a = a.model().agent("local-A").build();
        let b = b.model().agent("local-B").build();
        for (label, agent) in [("A1", &a), ("A2", &a), ("B1", &b)] {
            let response = agent.prompt(format!("{label}: {prompt}")).await?;
            if let Ok(expected) = std::env::var("ACP_EXPECT_TEXT") {
                anyhow::ensure!(response.to_string() == expected, "unexpected peer response");
            }
            println!("{label} Rig result: {response}");
        }
    }
    println!("PASS peer smoke");
    Ok(())
}
