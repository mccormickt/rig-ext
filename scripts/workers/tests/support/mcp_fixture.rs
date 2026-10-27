use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    runtime::Runtime,
};

#[derive(Default)]
struct State {
    requests: BTreeMap<String, usize>,
    closed: BTreeSet<String>,
    started: BTreeSet<String>,
    records: Vec<Value>,
    effects: usize,
    errors: Vec<String>,
}

pub struct Fixture {
    state: Arc<Mutex<State>>,
    _runtime: Runtime,
}

impl Fixture {
    pub fn start() -> Self {
        let runtime = Runtime::new().unwrap();
        let listener = runtime
            .block_on(TcpListener::bind("127.0.0.1:9899"))
            .expect("MCP fixture port 9899 must be free");
        let state = Arc::new(Mutex::new(State::default()));
        let shared = state.clone();
        runtime.spawn(async move {
            loop {
                let (socket, _) = listener.accept().await.unwrap();
                let state = shared.clone();
                tokio::spawn(async move {
                    if let Err(error) = serve(socket, &state).await {
                        state.lock().unwrap().errors.push(error.to_string());
                    }
                });
            }
        });
        Self {
            state,
            _runtime: runtime,
        }
    }

    pub fn reset(&self) {
        let mut state = self.state.lock().unwrap();
        assert!(
            state.errors.is_empty(),
            "fixture errors: {:?}",
            state.errors
        );
        *state = State::default();
    }

    pub fn stats(&self) -> Value {
        let state = self.state.lock().unwrap();
        assert!(
            state.errors.is_empty(),
            "fixture errors: {:?}",
            state.errors
        );
        json!({"requests": state.requests, "closed": state.closed, "started": state.started,
            "records": state.records, "effects": state.effects})
    }
}

type Error = Box<dyn std::error::Error + Send + Sync>;

async fn response(
    socket: &mut TcpStream,
    status: u16,
    body: &str,
    extra: &str,
) -> Result<(), Error> {
    socket.write_all(format!("HTTP/1.1 {status} Fixture\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n{extra}\r\n{body}", body.len()).as_bytes()).await?;
    Ok(())
}

async fn serve(mut socket: TcpStream, shared: &Arc<Mutex<State>>) -> Result<(), Error> {
    let mut bytes = Vec::new();
    let end = loop {
        let mut buf = [0; 4096];
        let n = tokio::time::timeout(Duration::from_secs(5), socket.read(&mut buf)).await??;
        if n == 0 {
            return Ok(());
        }
        bytes.extend_from_slice(&buf[..n]);
        if let Some(index) = bytes.windows(4).position(|b| b == b"\r\n\r\n") {
            break index + 4;
        }
        if bytes.len() > 65536 {
            return Err("fixture headers too large".into());
        }
    };
    let headers = std::str::from_utf8(&bytes[..end])?;
    let mut lines = headers.lines();
    let mut request_line = lines
        .next()
        .ok_or("missing request line")?
        .split_whitespace();
    let method = request_line.next();
    let path = request_line
        .next()
        .ok_or("missing request path")?
        .trim_start_matches('/')
        .to_owned();
    if method == Some("GET") && path == "stats" {
        let stats = {
            let state = shared.lock().unwrap();
            json!({"requests": state.requests, "closed": state.closed, "started": state.started,
                "records": state.records, "effects": state.effects})
        };
        return response(&mut socket, 200, &stats.to_string(), "").await;
    }
    if method != Some("POST") {
        return Err("fixture expects POST".into());
    }
    let headers: BTreeMap<_, _> = lines
        .filter_map(|line| line.split_once(':'))
        .map(|(k, v)| (k.to_ascii_lowercase(), v.trim().to_owned()))
        .collect();
    let length: usize = headers
        .get("content-length")
        .ok_or("fixture needs content-length")?
        .parse()?;
    if length > 1024 * 1024 {
        return Err("fixture body too large".into());
    }
    while bytes.len() < end + length {
        let mut buf = [0; 4096];
        let n = tokio::time::timeout(Duration::from_secs(5), socket.read(&mut buf)).await??;
        if n == 0 {
            return Err("truncated fixture request".into());
        }
        bytes.extend_from_slice(&buf[..n]);
    }
    let body: Value = serde_json::from_slice(&bytes[end..end + length])?;
    for (header, value) in [
        ("authorization", "Bearer conformance-only"),
        ("accept", "application/json"),
        ("mcp-protocol-version", "2026-07-28"),
        (
            "mcp-method",
            body["method"].as_str().ok_or("missing method")?,
        ),
    ] {
        if headers.get(header).map(String::as_str) != Some(value) {
            return Err(format!("incorrect {header}").into());
        }
    }
    if headers.contains_key("mcp-session-id") {
        return Err("unexpected session".into());
    }
    let meta = &body["params"]["_meta"];
    if meta["io.modelcontextprotocol/protocolVersion"] != "2026-07-28"
        || meta["io.modelcontextprotocol/clientCapabilities"] != json!({})
        || !matches!(
            meta["io.modelcontextprotocol/clientInfo"]["name"].as_str(),
            Some("native-example" | "worker-conformance")
        )
    {
        return Err("incorrect protocol metadata".into());
    }
    let record = {
        let mut state = shared.lock().unwrap();
        *state.requests.entry(path.clone()).or_default() += 1;
        state.records.push(json!({"path": path, "client": meta["io.modelcontextprotocol/clientInfo"]["name"],
            "id": body["id"], "method": body["method"], "arguments": body["params"]["arguments"],
            "protocol": headers["mcp-protocol-version"], "regionHeader": headers.get("mcp-param-region")}));
        state.records.len() - 1
    };
    if body["method"] == "tools/list" {
        let second = body["params"]["cursor"] == "second";
        let mut result = json!({"resultType": "complete", "ttlMs": 0, "cacheScope": "private", "tools": [{
            "name": if second {"other"} else {"lookup"},
            "inputSchema": {"type": "object", "properties": {"region": {"type": "string", "x-mcp-header": "Region"}}, "required": ["region"]},
            "outputSchema": {"type": "object", "properties": {"answer": {"type": "integer"}}, "required": ["answer"]}
        }]});
        if !second {
            result["nextCursor"] = json!("second");
        }
        send_result(&mut socket, shared, record, &body["id"], result).await?;
    } else {
        if body["method"] != "tools/call"
            || headers.get("mcp-name").map(String::as_str) != body["params"]["name"].as_str()
            || headers.get("mcp-param-region").map(String::as_str)
                != body["params"]["arguments"]["region"].as_str()
        {
            return Err("incorrect tool call headers".into());
        }
        shared.lock().unwrap().effects += 1;
        match path.as_str() {
            "disconnect" => {},
            "timeout" | "sse" | "http-html" | "http-timeout" | "cancel-headers" | "cancel-body" => {
                if path != "cancel-headers" {
                    let media = match path.as_str() { "sse" => "text/event-stream", "http-html" => "text/html", _ => "application/json" };
                    let status = if path.starts_with("http-") {503} else {200};
                    socket.write_all(format!("HTTP/1.1 {status} Fixture\r\nContent-Type: {media}\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n").as_bytes()).await?;
                    if media == "application/json" { socket.write_all(b"1\r\n{\r\n").await?; }
                }
                shared.lock().unwrap().started.insert(path.clone());
                let mut byte = [0];
                match tokio::time::timeout(Duration::from_secs(10), socket.read(&mut byte)).await {
                    Ok(Ok(0)) => {},
                    Ok(Err(error)) if error.kind() == std::io::ErrorKind::ConnectionReset => {},
                    _ => return Err(format!("client did not close stalled {path}").into()),
                }
            },
            "oversize" | "http-oversize" => {
                let status = if path == "oversize" {200} else {503};
                socket.write_all(format!("HTTP/1.1 {status} Fixture\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n2000\r\n{}\r\n0\r\n\r\n", " ".repeat(8192)).as_bytes()).await?;
            },
            "http-empty" => socket.write_all(b"HTTP/1.1 401 Fixture\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").await?,
            "http-json" => response(&mut socket, 429, r#"{"message":"private-marker"}"#, "").await?,
            "http-malformed" => response(&mut socket, 502, "{", "").await?,
            "http-missing-id" | "http-wrong-id" => {
                let mut reply = json!({"jsonrpc": "2.0", "error": {"code": -32022, "message": "private-marker"}});
                if path == "http-wrong-id" { reply["id"] = json!("wrong"); }
                response(&mut socket, 403, &reply.to_string(), "").await?;
            },
            "redirect" => response(&mut socket, 307, &json!({"jsonrpc":"2.0", "id": body["id"], "result": {"resultType":"complete", "content":[]}}).to_string(), "Location: /followed\r\n").await?,
            "rpc" => response(&mut socket, 400, &json!({"jsonrpc":"2.0", "id": body["id"], "error": {"code": -32022, "message":"private-marker"}}).to_string(), "").await?,
            _ => {
                let answer = if path == "bad-schema" {json!("wrong")} else if body["params"]["arguments"]["region"] == "east" {json!(12)} else {json!(37)};
                let output = json!({"answer": answer});
                let result = json!({"resultType":"complete", "isError": path == "is-error",
                    "content": [{"type":"text", "text": serde_json::to_string_pretty(&output)?, "_meta":{"secret":"private-marker"}}],
                    "structuredContent":output, "_meta":{"secret":"private-marker"}});
                let id = if path == "mismatch" {json!("wrong")} else {body["id"].clone()};
                send_result(&mut socket, shared, record, &id, result).await?;
            }
        }
    }
    shared.lock().unwrap().closed.insert(path);
    Ok(())
}

async fn send_result(
    socket: &mut TcpStream,
    shared: &Arc<Mutex<State>>,
    record: usize,
    id: &Value,
    result: Value,
) -> Result<(), Error> {
    {
        let mut state = shared.lock().unwrap();
        state.records[record]["status"] = json!(200);
        state.records[record]["result"] = result.clone();
    }
    response(
        socket,
        200,
        &json!({"jsonrpc":"2.0", "id":id, "result":result}).to_string(),
        "",
    )
    .await
}
