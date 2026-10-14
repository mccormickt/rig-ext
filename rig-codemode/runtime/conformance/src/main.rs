use std::io::{self, BufRead};
use worker_replay_conformance::{Input, output};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    for line in io::stdin().lock().lines() {
        let input: Input = serde_json::from_str(&line?)?;
        let history = input.history();
        let result = rig_codemode::replay::step(
            &input.code,
            &input.catalog,
            input.limits,
            &input.admitted,
            &history,
        )
        .await;
        println!("{}", output(result));
    }
    Ok(())
}
