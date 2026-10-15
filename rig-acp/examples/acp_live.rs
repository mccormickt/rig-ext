//! Synthetic live-peer checks. The Python harness owns configuration and receipts.
use std::{collections::HashMap, path::Path, sync::Arc, time::Duration};

use rig_acp::{
    Client, Session,
    protocol::{AcpAgent, AcpAgentConfig},
    schema::*,
};
use serde_json::{Value, json};

fn receipt(mut value: Value) {
    value["time"] = json!(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .ok()
            .map(|d| d.as_secs_f64())
    );
    println!("{value}");
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tokio::time::timeout(Duration::from_secs(600), run()).await?
}

async fn run() -> anyhow::Result<()> {
    let plan: Value = serde_json::from_slice(&std::fs::read(std::env::var("ACP_LIVE_PLAN")?)?)?;
    let client = if let Ok(endpoint) = std::env::var("ACP_RIVET_URL") {
        #[cfg(feature = "rivet")]
        {
            let mut headers = reqwest::header::HeaderMap::new();
            let mut token = reqwest::header::HeaderValue::from_str(&format!(
                "Bearer {}",
                std::env::var("ACP_RIVET_TOKEN")?
            ))?;
            token.set_sensitive(true);
            headers.insert(reqwest::header::AUTHORIZATION, token);
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
            anyhow::bail!("enable rivet");
        }
    } else {
        let mut args = std::env::args().skip(1);
        let executable = args
            .next()
            .ok_or_else(|| anyhow::anyhow!("missing executable"))?;
        Client::connect(AcpAgent::new(AcpAgentConfig::new(executable).args(args))).await?
    };
    receipt(json!({"event":"initialized", "value":client.initialized()}));
    let mut sessions: HashMap<String, Session> = HashMap::new();
    for action in plan
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("plan must be an array"))?
    {
        let name = action["session"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("missing session"))?;
        if !sessions.contains_key(name) {
            let session = client.new_session(std::env::current_dir()?).await?;
            receipt(json!({"event":"session", "name":name, "id":session.id()}));
            sessions.insert(name.to_owned(), session);
        }
        let session = &sessions[name];
        let label = action["label"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("missing label"))?;
        let prompt = action["prompt"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("missing prompt"))?;
        receipt(json!({"event":"begin", "label":label, "session":session.id()}));
        let action_owned = action.clone();
        let decisions = Arc::new(std::sync::Mutex::new(Vec::new()));
        let observed = decisions.clone();
        let model = session.model().permission_policy(move |request| {
            let action = action_owned.clone();
            let observed = observed.clone();
            async move {
                let command = request
                    .tool_call
                    .fields
                    .raw_input
                    .as_ref()
                    .and_then(|v| v.get("command"))
                    .and_then(Value::as_str);
                let matches = command.is_some() && command == action["command"].as_str();
                let held = action["target"]
                    .as_str()
                    .map(|p| {
                        std::fs::read_to_string(p).ok().as_deref() == Some("disposable-probe\n")
                    })
                    .unwrap_or(false);
                tokio::time::sleep(Duration::from_millis(150)).await;
                let id = if matches && held && action["allow"] == true {
                    "allow_once"
                } else {
                    "deny"
                };
                let offered = request
                    .options
                    .iter()
                    .any(|o| o.option_id.to_string() == id);
                let record = json!({"event":"permission", "label":action["label"],
                    "session":request.session_id, "command":command, "matches":matches,
                    "held":held, "selected":id, "offered":offered});
                receipt(record.clone());
                if let Ok(mut decisions) = observed.lock() {
                    decisions.push(record);
                }
                if offered {
                    RequestPermissionOutcome::Selected(SelectedPermissionOutcome::new(id))
                } else {
                    RequestPermissionOutcome::Cancelled
                }
            }
        });
        let agent = model.agent(format!("live-{name}")).build();
        if action["bad_route"] == true {
            let result = agent.prompt(prompt).using_model("absent-live-route").await;
            anyhow::ensure!(result.is_err(), "invalid route succeeded");
            receipt(json!({"event":"invalid_route_rejected", "label":label}));
            continue;
        }
        let result = if let Some(started) = action["cancel_started"].as_str() {
            let cancel = async {
                tokio::time::timeout(Duration::from_secs(120), async {
                    while !Path::new(started).exists() {
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    }
                })
                .await?;
                let pid = std::fs::read_to_string(started)?.trim().parse::<u32>()?;
                let identity = std::fs::read_to_string(format!("/proc/{pid}/stat"))?;
                let children = std::fs::read_to_string(format!("/proc/{pid}/task/{pid}/children"))?;
                let descendants: Vec<u32> = children
                    .split_whitespace()
                    .map(str::parse)
                    .collect::<Result<_, _>>()?;
                anyhow::ensure!(!descendants.is_empty(), "no running command descendant");
                receipt(json!({"event":"cancel_sent", "label":label, "pid":pid,
                    "process_stat":identity, "children":descendants, "session":session.id()}));
                session.cancel()?;
                tokio::time::timeout(Duration::from_secs(4), async {
                    while std::iter::once(&pid)
                        .chain(&descendants)
                        .any(|p| Path::new(&format!("/proc/{p}")).exists())
                    {
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    }
                })
                .await?;
                receipt(json!({"event":"command_exited", "label":label, "pid":pid}));
                tokio::time::sleep(Duration::from_secs(9)).await;
                anyhow::ensure!(
                    !Path::new("cancel-finished").exists(),
                    "cancelled effect happened"
                );
                receipt(json!({"event":"effect_absent_before_cleanup", "label":label}));
                Ok::<_, anyhow::Error>(())
            };
            let (result, cancelled) = tokio::join!(agent.prompt(prompt).into_future(), cancel);
            cancelled?;
            anyhow::ensure!(result.is_err(), "cancelled Rig prompt must fail");
            result
        } else {
            agent.prompt(prompt).await
        };
        receipt(json!({"event":"result", "label":label, "ok":result.is_ok(),
            "text":result.as_ref().map(ToString::to_string).ok(),
            "error":result.as_ref().err().map(ToString::to_string)}));
        if action["cancel_started"].is_null() && action["failure_control"] != true {
            result?;
        }
        let decisions = decisions
            .lock()
            .map_err(|_| anyhow::anyhow!("permission lock"))?;
        if action["target"].is_string() {
            anyhow::ensure!(!decisions.is_empty(), "no real permission request");
            anyhow::ensure!(
                decisions
                    .iter()
                    .all(|d| d["matches"] == true && d["held"] == true && d["offered"] == true),
                "unexpected permission"
            );
            let target = action["target"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("target"))?;
            anyhow::ensure!(
                Path::new(target).exists() != (action["allow"] == true),
                "wrong permission effect"
            );
        }
        receipt(json!({"event":"end", "label":label}));
    }
    receipt(json!({"event":"owners_alive_checks_complete"}));
    Ok(())
}
