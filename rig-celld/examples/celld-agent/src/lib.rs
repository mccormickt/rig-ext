use rig::{
    AgentBuilder,
    completion::Message,
    providers::openai::{self, OpenAI},
};
use rig_celld::SqliteVecIndex;
use serde::{Deserialize, Serialize};
use worker::*;

const HISTORY_KEY: &str = "history";
const DEFAULT_COMPLETION_MODEL: &str = openai::GPT_4O_MINI;
const DEFAULT_EMBEDDING_MODEL: &str = openai::TEXT_EMBEDDING_3_SMALL;
const DEFAULT_EMBEDDING_DIMENSIONS: usize = 512;

#[derive(Debug, Deserialize)]
struct PromptInput {
    prompt: String,
}

#[derive(Debug, Serialize)]
struct PromptOutput {
    answer: String,
    turn: i64,
}

#[derive(Debug, Serialize)]
struct Memory {
    id: String,
    prompt: String,
    answer: String,
}

#[derive(Debug, Deserialize)]
struct StateRow {
    value: String,
}

#[derive(Debug, Deserialize)]
struct TurnRow {
    id: i64,
}

#[durable_object(fetch)]
pub struct AgentCell {
    state: State,
    env: Env,
}

impl DurableObject for AgentCell {
    fn new(state: State, env: Env) -> Self {
        Self { state, env }
    }

    async fn fetch(&self, mut req: Request) -> Result<Response> {
        if req.method() != Method::Post {
            return Response::error("Send a POST request with a JSON prompt.\n", 405);
        }

        let input: PromptInput = req.json().await?;
        if input.prompt.trim().is_empty() {
            return Response::error("The prompt must not be empty.\n", 400);
        }

        let sql = self.state.storage().sql();
        initialize_state(&sql)?;
        let mut history = load_history(&sql)?;

        let api_key = self.env.var("OPENAI_API_KEY")?.to_string();
        let completion_model = optional_var(
            &self.env,
            "OPENAI_COMPLETION_MODEL",
            DEFAULT_COMPLETION_MODEL,
        );
        let embedding_model =
            optional_var(&self.env, "OPENAI_EMBEDDING_MODEL", DEFAULT_EMBEDDING_MODEL);
        let embedding_dimensions = optional_var(
            &self.env,
            "OPENAI_EMBEDDING_DIMENSIONS",
            &DEFAULT_EMBEDDING_DIMENSIONS.to_string(),
        )
        .parse::<usize>()
        .map_err(to_worker_error)?;

        let client = OpenAI::new(api_key);
        let embedding_model = client.embedding(embedding_model, Some(embedding_dimensions));
        let index = SqliteVecIndex::new(sql.clone(), embedding_model).map_err(to_worker_error)?;
        let memory_index = index.clone();
        let agent = AgentBuilder::new(client.completion(completion_model))
            .preamble(
                "You are a durable assistant. Use the retrieved memories when they are relevant. \
                 Do not claim that a memory is current when the user has corrected it.",
            )
            .dynamic_context(5, index)
            .build();

        let answer = agent
            .chat(input.prompt.clone(), &mut history)
            .await
            .map_err(to_worker_error)?
            .output;
        let turn = insert_turn(&sql, &input.prompt, &answer)?;
        let memory = Memory {
            id: turn.to_string(),
            prompt: input.prompt.clone(),
            answer: answer.clone(),
        };
        let memory_text = format!("User: {}\nAssistant: {}", input.prompt, answer);
        memory_index
            .upsert_text(&memory.id, &memory, &memory_text)
            .await
            .map_err(to_worker_error)?;
        save_history(&sql, &history)?;

        Response::from_json(&PromptOutput { answer, turn })
    }
}

#[event(fetch)]
async fn fetch(req: Request, env: Env, _ctx: Context) -> Result<Response> {
    let path = req.path();
    let Some(agent_name) = agent_name(&path) else {
        let status = if path == "/" { 200 } else { 404 };
        return Ok(
            Response::ok("POST {\"prompt\":\"...\"} to /agents/{name}/messages\n")?
                .with_status(status),
        );
    };

    let namespace = env.durable_object("AGENTS")?;
    namespace
        .id_from_name(agent_name)?
        .get_stub()?
        .fetch_with_request(req)
        .await
}

fn agent_name(path: &str) -> Option<&str> {
    let mut segments = path.strip_prefix('/')?.split('/');
    match (
        segments.next(),
        segments.next(),
        segments.next(),
        segments.next(),
    ) {
        (Some("agents"), Some(name), Some("messages"), None) if !name.is_empty() => Some(name),
        _ => None,
    }
}

fn initialize_state(sql: &SqlStorage) -> Result<()> {
    sql.exec(
        "CREATE TABLE IF NOT EXISTS agent_state(\
         key TEXT PRIMARY KEY, value TEXT NOT NULL)",
        None,
    )?;
    sql.exec(
        "CREATE TABLE IF NOT EXISTS turns(\
         id INTEGER PRIMARY KEY AUTOINCREMENT, \
         prompt TEXT NOT NULL, answer TEXT NOT NULL)",
        None,
    )?;
    Ok(())
}

fn load_history(sql: &SqlStorage) -> Result<Vec<Message>> {
    let rows = sql
        .exec(
            "SELECT value FROM agent_state WHERE key = ?",
            vec![SqlStorageValue::String(HISTORY_KEY.to_owned())],
        )?
        .to_array::<StateRow>()?;

    match rows.into_iter().next() {
        Some(row) => serde_json::from_str(&row.value).map_err(Error::from),
        None => Ok(Vec::new()),
    }
}

fn save_history(sql: &SqlStorage, history: &[Message]) -> Result<()> {
    let value = serde_json::to_string(history)?;
    sql.exec(
        "INSERT INTO agent_state(key, value) VALUES (?, ?) \
         ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        vec![
            SqlStorageValue::String(HISTORY_KEY.to_owned()),
            SqlStorageValue::String(value),
        ],
    )?;
    Ok(())
}

fn insert_turn(sql: &SqlStorage, prompt: &str, answer: &str) -> Result<i64> {
    let row = sql
        .exec(
            "INSERT INTO turns(prompt, answer) VALUES (?, ?) RETURNING id",
            vec![
                SqlStorageValue::String(prompt.to_owned()),
                SqlStorageValue::String(answer.to_owned()),
            ],
        )?
        .one::<TurnRow>()?;
    Ok(row.id)
}

fn optional_var(env: &Env, name: &str, default: &str) -> String {
    env.var(name)
        .map(|value| value.to_string())
        .unwrap_or_else(|_| default.to_owned())
}

fn to_worker_error(error: impl std::fmt::Display) -> Error {
    Error::RustError(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::agent_name;

    #[test]
    fn parses_only_message_routes() {
        assert_eq!(agent_name("/agents/demo/messages"), Some("demo"));
        assert_eq!(agent_name("/agents/demo"), None);
        assert_eq!(agent_name("/agents//messages"), None);
        assert_eq!(agent_name("/agents/demo/messages/extra"), None);
    }
}
