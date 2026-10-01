use std::{
    io,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use rig::{
    test_utils::{MockCompletionModel, MockTurn},
    tool::{Tool, ToolContext, ToolExecutionError},
};
use rig_durable::{
    ToolInvocation,
    temporal::{TemporalAgent, TemporalAgentWorkflow},
};
use serde::Deserialize;
use temporalio_client::{
    Client, ClientOptions, Connection, WorkflowGetResultOptions, WorkflowStartOptions,
    envconfig::LoadClientConfigProfileOptions,
};
use temporalio_sdk::{Runtime, Worker, WorkerOptions};

#[derive(Clone)]
struct FlakyLookup {
    attempts: Arc<AtomicUsize>,
    invocations: Arc<Mutex<Vec<ToolInvocation>>>,
}

#[derive(Deserialize)]
struct LookupArgs {
    key: String,
}

impl Tool for FlakyLookup {
    const NAME: &'static str = "lookup";
    type Args = LookupArgs;
    type Output = String;
    type Error = io::Error;

    fn description(&self) -> String {
        "Look up a value from a temporarily unreliable service".into()
    }

    fn parameters(&self) -> serde_json::Value {
        serde_json::json!({
            "type": "object",
            "properties": {"key": {"type": "string"}},
            "required": ["key"]
        })
    }

    fn map_error(&self, error: Self::Error) -> ToolExecutionError {
        ToolExecutionError::provider(error.to_string()).with_source(error)
    }

    async fn call(
        &self,
        context: &mut ToolContext,
        args: LookupArgs,
    ) -> Result<Self::Output, Self::Error> {
        self.invocations
            .lock()
            .unwrap()
            .push(context.require::<ToolInvocation>().unwrap().clone());
        let attempt = self.attempts.fetch_add(1, Ordering::SeqCst) + 1;
        println!("lookup activity attempt {attempt}");
        if attempt == 1 {
            return Err(io::Error::other("temporary upstream failure"));
        }
        Ok(format!("{}=available", args.key))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = Runtime::from_current_tokio(Default::default())?;
    let (connection_options, client_options) =
        ClientOptions::load_from_config(LoadClientConfigProfileOptions::default())?;
    let connection = Connection::connect(connection_options).await?;
    let client = Client::new(connection, client_options)?;
    let attempts = Arc::new(AtomicUsize::new(0));
    let invocations = Arc::new(Mutex::new(Vec::new()));
    let agent = TemporalAgent::new(MockCompletionModel::from_turns([
        MockTurn::tool_call("lookup-1", "lookup", serde_json::json!({"key": "service"})),
        MockTurn::text("The service is available."),
    ]))
    .activity_max_attempts(3)
    .tool(FlakyLookup {
        attempts: Arc::clone(&attempts),
        invocations: Arc::clone(&invocations),
    });
    let input = agent.input("Check whether the service is available.");
    let task_queue = format!("rig-temporal-retry-{}", uuid::Uuid::new_v4());
    let mut options = WorkerOptions::new(task_queue.clone()).build();
    agent.register(&mut options)?;
    let mut worker = Worker::new(&runtime, client.clone(), options)?;
    let shutdown = worker.shutdown_handle();
    let run = async move {
        let handle = client
            .start_workflow(
                TemporalAgentWorkflow::run,
                input,
                WorkflowStartOptions::new(
                    task_queue,
                    format!("rig-temporal-retry-{}", uuid::Uuid::new_v4()),
                )
                .build(),
            )
            .await?;
        let response = handle
            .get_result(WorkflowGetResultOptions::default())
            .await?;
        shutdown();
        Ok::<_, Box<dyn std::error::Error>>(response)
    };
    let (worker_result, response) = tokio::join!(worker.run(), run);
    worker_result?;
    let response = response?;

    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    let invocations = invocations.lock().unwrap();
    assert_eq!(invocations.len(), 2);
    assert_eq!(invocations[0], invocations[1]);
    assert_eq!(response.output, "The service is available.");
    println!("{}", response.output);
    Ok(())
}
