#![cfg(all(feature = "mcp-http", not(target_family = "wasm")))]
#![allow(clippy::unwrap_used)]

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use rig_celld::mcp::{Client, Error, HttpTransport, Limits, model::Implementation};
use serde_json::{Value, json};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::Notify,
    task::{JoinHandle, JoinSet},
};

struct Server {
    url: String,
    requests: Arc<Mutex<BTreeMap<String, usize>>>,
    started: Arc<Notify>,
    closed: Arc<Notify>,
    task: JoinHandle<()>,
}

impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Server {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let requests = Arc::new(Mutex::new(BTreeMap::new()));
        let counts = requests.clone();
        let started = Arc::new(Notify::new());
        let closed = Arc::new(Notify::new());
        let (sent, disconnected) = (started.clone(), closed.clone());
        let task = tokio::spawn(async move {
            let mut connections = JoinSet::new();
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let counts = counts.clone();
                let (sent, disconnected) = (sent.clone(), disconnected.clone());
                connections.spawn(async move {
                    let mut bytes = Vec::new();
                    let end = loop {
                        let mut buffer = [0; 4096];
                        let n = socket.read(&mut buffer).await.unwrap();
                        if n == 0 { return; }
                        bytes.extend_from_slice(&buffer[..n]);
                        if let Some(end) = bytes.windows(4).position(|w| w == b"\r\n\r\n") { break end + 4; }
                    };
                    let headers = String::from_utf8(bytes[..end].to_vec()).unwrap();
                    let path = headers.split_whitespace().nth(1).unwrap().to_owned();
                    *counts.lock().unwrap().entry(path.clone()).or_default() += 1;
                    if path == "/schema" {
                        let schema = r#"{"type":"object"}"#;
                        socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}", schema.len(), schema).as_bytes()).await.unwrap();
                        return;
                    }
                    let length: usize = headers.lines().find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length").then(|| value.trim().parse().unwrap())
                    }).unwrap();
                    while bytes.len() - end < length {
                        let mut buffer = [0; 4096];
                        let n = socket.read(&mut buffer).await.unwrap();
                        if n == 0 { return; }
                        bytes.extend_from_slice(&buffer[..n]);
                    }
                    let request: Value = serde_json::from_slice(&bytes[end..end + length]).unwrap();
                    if matches!(path.as_str(), "/cancel-headers" | "/cancel-body") {
                        if path == "/cancel-body" {
                            socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\n\r\n1\r\n{\r\n").await.unwrap();
                        }
                        sent.notify_one();
                        let mut byte = [0];
                        if matches!(socket.read(&mut byte).await, Ok(0) | Err(_)) {
                            disconnected.notify_one();
                        }
                        return;
                    }
                    if let Some(status_case) = path.strip_prefix("/status/") {
                        let (status, case) = status_case.split_once('/').unwrap();
                        let (content_type, body) = match case {
                            "empty" => ("", String::new()),
                            "html" => ("Content-Type: text/html\r\n", "<h1>private detail</h1>".into()),
                            "json" => ("Content-Type: application/json\r\n", json!({"message": "private detail"}).to_string()),
                            "malformed" => ("Content-Type: application/json\r\n", "{".into()),
                            "oversize" => ("Content-Type: application/json\r\n", " ".repeat(2048)),
                            _ => {
                                let mut envelope = json!({"jsonrpc": "2.0", "id": request["id"], "error": {"code": -32022, "message": "private detail"}});
                                match case {
                                    "missing-id" => { envelope.as_object_mut().unwrap().remove("id"); }
                                    "null-id" => envelope["id"] = Value::Null,
                                    "wrong-id" => envelope["id"] = json!("wrong"),
                                    "wrong-version" => envelope["jsonrpc"] = json!("1.0"),
                                    "both" => envelope["result"] = json!({}),
                                    "bad-code" => envelope["error"]["code"] = json!("wrong"),
                                    "bad-message" => envelope["error"]["message"] = Value::Null,
                                    "rpc" => {},
                                    _ => unreachable!(),
                                }
                                ("Content-Type: application/json\r\n", envelope.to_string())
                            }
                        };
                        socket.write_all(format!("HTTP/1.1 {status} Test\r\n{content_type}Content-Length: {}\r\n\r\n{}", body.len(), body).as_bytes()).await.unwrap();
                        return;
                    }
                    let success = json!({"jsonrpc": "2.0", "id": request["id"], "result": {"resultType": "complete", "tools": []}}).to_string();
                    let response = match path.as_str() {
                        "/disconnect" => return,
                        "/timeout" => "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\n\r\n1\r\n{\r\n".into(),
                        "/sse" => "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n".into(),
                        "/redirect" => format!("HTTP/1.1 307 Temporary Redirect\r\nLocation: /followed\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}", success.len(), success),
                        "/oversized" => format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\n\r\n1000\r\n{}\r\n0\r\n\r\n", " ".repeat(4096)),
                        "/rpc" => {
                            let error = json!({"jsonrpc": "2.0", "id": request["id"], "error": {"code": -32022, "message": "private server detail"}}).to_string();
                            format!("HTTP/1.1 400 Bad Request\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}", error.len(), error)
                        }
                        _ => format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}", success.len(), success),
                    };
                    let _ = socket.write_all(response.as_bytes()).await;
                    if matches!(path.as_str(), "/sse" | "/timeout") { tokio::time::sleep(Duration::from_secs(10)).await; }
                });
            }
        });
        Self {
            url,
            requests,
            started,
            closed,
            task,
        }
    }

    fn client(&self, path: &str) -> Client<HttpTransport> {
        Client::new(
            HttpTransport::new(&format!("{}{path}", self.url)).unwrap(),
            Implementation::new("test", "1"),
        )
        .with_limits(Limits {
            response_bytes: 1024,
            timeout: Duration::from_millis(200),
            ..Limits::default()
        })
        .unwrap()
    }
}

#[tokio::test]
async fn http_bounds_deadlines_and_single_attempt_errors() {
    let server = Server::start().await;
    assert!(server.client("/ok").list_tools().await.unwrap().is_empty());
    assert!(matches!(
        server.client("/rpc").list_tools().await,
        Err(Error::Rpc(-32022))
    ));
    assert!(matches!(
        server.client("/redirect").list_tools().await,
        Err(Error::Http(307))
    ));
    assert!(matches!(
        server.client("/sse").list_tools().await,
        Err(Error::ContentType)
    ));
    assert!(matches!(
        server.client("/oversized").list_tools().await,
        Err(Error::Limit("response bytes"))
    ));
    assert!(matches!(
        server.client("/timeout").list_tools().await,
        Err(Error::Timeout)
    ));
    assert!(matches!(
        server.client("/disconnect").list_tools().await,
        Err(Error::Transport)
    ));
    let requests = server.requests.lock().unwrap();
    assert_eq!(requests.len(), 7);
    assert!(requests.values().all(|count| *count == 1));
    assert!(!requests.contains_key("/followed"));
}

#[tokio::test]
async fn http_errors_preserve_status_unless_the_rpc_error_is_correlated() {
    let server = Server::start().await;
    for status in [200, 401, 403, 429, 503] {
        for case in [
            "empty",
            "html",
            "json",
            "malformed",
            "missing-id",
            "null-id",
            "wrong-id",
            "wrong-version",
            "both",
            "bad-code",
            "bad-message",
            "rpc",
            "oversize",
        ] {
            let path = format!("/status/{status}/{case}");
            let result = server.client(&path).list_tools().await;
            let passed = match case {
                "oversize" => matches!(result, Err(Error::Limit("response bytes"))),
                "rpc" => matches!(result, Err(Error::Rpc(-32022))),
                _ if status != 200 => matches!(result, Err(Error::Http(code)) if code == status),
                "empty" | "html" => matches!(result, Err(Error::ContentType)),
                _ => matches!(result, Err(Error::Protocol(_))),
            };
            assert!(passed, "unexpected result for {path}");
        }
    }
    let requests = server.requests.lock().unwrap();
    assert_eq!(requests.len(), 65);
    assert!(requests.values().all(|count| *count == 1));
}

#[tokio::test]
async fn schemas_cannot_read_files_or_http_with_unified_resolver_features() {
    let server = Server::start().await;
    let file = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(file.path(), r#"{"type":"object"}"#).unwrap();
    let refs = [
        url::Url::from_file_path(file.path()).unwrap().to_string(),
        format!("{}/schema", server.url),
    ];
    let client = server.client("/unused");
    tokio::task::spawn_blocking(move || {
        for reference in refs {
            let external = json!({"$ref": reference});
            // This control requires the unified HTTP/file resolver features.
            assert!(
                jsonschema::validator_for(&external)
                    .unwrap()
                    .is_valid(&json!({}))
            );
            for field in ["inputSchema", "outputSchema"] {
                let mut definition = json!({"name": "lookup", "inputSchema": {"type": "object"}});
                definition[field] = external.clone();
                assert!(
                    matches!(
                        client.tool(serde_json::from_value(definition).unwrap()),
                        Err(Error::Schema)
                    ),
                    "external {field} was accepted"
                );
            }
        }
        let local = json!({"$defs": {"object": {"type": "object"}}, "$ref": "#/$defs/object"});
        assert!(
            client
                .tool(
                    serde_json::from_value(
                        json!({"name": "lookup", "inputSchema": local, "outputSchema": local})
                    )
                    .unwrap()
                )
                .is_ok()
        );
    })
    .await
    .unwrap();
    assert_eq!(
        *server.requests.lock().unwrap(),
        BTreeMap::from([("/schema".into(), 1)])
    );
}

#[tokio::test]
async fn dropping_active_requests_closes_io_before_the_deadline_without_retry() {
    let server = Server::start().await;
    for path in ["/cancel-headers", "/cancel-body"] {
        let client = server
            .client(path)
            .with_limits(Limits {
                timeout: Duration::from_secs(30),
                ..Limits::default()
            })
            .unwrap();
        let call = tokio::spawn(async move { client.list_tools().await });
        tokio::time::timeout(Duration::from_secs(2), server.started.notified())
            .await
            .unwrap();
        call.abort();
        assert!(call.await.is_err_and(|error| error.is_cancelled()));
        tokio::time::timeout(Duration::from_secs(2), server.closed.notified())
            .await
            .unwrap();
    }
    assert_eq!(
        *server.requests.lock().unwrap(),
        BTreeMap::from([("/cancel-headers".into(), 1), ("/cancel-body".into(), 1)])
    );
}

#[test]
fn endpoint_and_credentials_are_validated_and_never_formatted() {
    for endpoint in [
        "file:///tmp/mcp",
        "https://user:password@example.org/mcp",
        "https://example.org/mcp#fragment",
    ] {
        assert!(HttpTransport::new(endpoint).is_err());
    }
    for token in [
        "",
        " leading",
        "trailing ",
        "secret\r\nx-injected: yes",
        "secret\tvalue",
    ] {
        assert!(
            HttpTransport::new("https://example.org/mcp")
                .unwrap()
                .bearer_auth(token)
                .is_err()
        );
    }
    let transport = HttpTransport::new("https://example.org/private?key=hidden")
        .unwrap()
        .bearer_auth("secret")
        .unwrap();
    assert_eq!(format!("{transport:?}"), "HttpTransport { .. }");
}
