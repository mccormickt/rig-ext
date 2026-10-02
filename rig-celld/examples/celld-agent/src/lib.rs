use rig::{
    AgentBuilder,
    completion::Message,
    providers::openai::{self, OpenAI, OpenAIConfig},
};
use rig_celld::{CellStorage, SqliteVecIndex};
use serde::{Deserialize, Serialize};
use worker::*;

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
struct MessageRow {
    message: String,
}

#[derive(Debug, Deserialize)]
struct TurnRow {
    id: i64,
}

#[durable_object(fetch)]
pub struct AgentCell {
    storage: CellStorage,
    env: Env,
}

impl DurableObject for AgentCell {
    fn new(state: State, env: Env) -> Self {
        Self {
            storage: state.into(),
            env,
        }
    }

    async fn fetch(&self, mut req: Request) -> Result<Response> {
        if req.method() != Method::Post {
            return Response::error("Send a POST request with a JSON prompt.\n", 405);
        }

        let input: PromptInput = req.json().await?;
        if input.prompt.trim().is_empty() {
            return Response::error("The prompt must not be empty.\n", 400);
        }

        let sql = self.storage.sql();
        self.storage.transaction_sync(|| initialize_state(&sql))?;
        let mut history = load_history(&sql)?;
        let loaded = history.len();

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

        let mut config = OpenAIConfig::new(api_key);
        if let Ok(base_url) = self.env.var("OPENAI_BASE_URL") {
            config = config.with_base_url(base_url.to_string());
        }
        let client: OpenAI = config.client();
        let embedding_model = client.embedding(embedding_model, Some(embedding_dimensions));
        let index = SqliteVecIndex::new(self.storage.clone(), embedding_model.clone())
            .map_err(to_worker_error)?;
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
        // Other requests to this agent can commit turns while this one waits
        // for the provider. Append only the messages of this turn, so a
        // concurrent turn is not overwritten.
        let messages = history.split_off(loaded);
        let memory_text = format!("User: {}\nAssistant: {}", input.prompt, answer);
        let embedding = embedding_model
            .embed_text(&memory_text)
            .await
            .map_err(to_worker_error)?;

        // The turn, its memory, and its messages commit together. The index
        // upsert runs as a nested transaction inside this one.
        let turn = self.storage.transaction_sync(|| {
            let turn = insert_turn(&sql, &input.prompt, &answer)?;
            let memory = Memory {
                id: turn.to_string(),
                prompt: input.prompt.clone(),
                answer: answer.clone(),
            };
            memory_index
                .upsert_embedding(&memory.id, &memory, &embedding)
                .map_err(to_worker_error)?;
            append_messages(&sql, turn, &messages)?;
            Ok::<_, Error>(turn)
        })?;

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
        "CREATE TABLE IF NOT EXISTS turns(\
         id INTEGER PRIMARY KEY AUTOINCREMENT, \
         prompt TEXT NOT NULL, answer TEXT NOT NULL)",
        None,
    )?;
    sql.exec(
        "CREATE TABLE IF NOT EXISTS messages(\
         id INTEGER PRIMARY KEY AUTOINCREMENT, \
         turn INTEGER NOT NULL REFERENCES turns(id), \
         message TEXT NOT NULL)",
        None,
    )?;
    Ok(())
}

fn load_history(sql: &SqlStorage) -> Result<Vec<Message>> {
    sql.exec("SELECT message FROM messages ORDER BY id", None)?
        .to_array::<MessageRow>()?
        .into_iter()
        .map(|row| serde_json::from_str(&row.message).map_err(Error::from))
        .collect()
}

fn append_messages(sql: &SqlStorage, turn: i64, messages: &[Message]) -> Result<()> {
    for message in messages {
        sql.exec(
            "INSERT INTO messages(turn, message) VALUES (?, ?)",
            vec![
                SqlStorageValue::Integer(turn),
                SqlStorageValue::String(serde_json::to_string(message)?),
            ],
        )?;
    }
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
