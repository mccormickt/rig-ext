// Loopback-only MCP fixture. Its bearer value is a public test constant.
import { createServer } from "node:http";
import assert from "node:assert/strict";

const requests = new Map();
const closed = new Set();
const started = new Set();
const records = [];
let effects = 0;
const definition = {
  name: "lookup",
  inputSchema: {
    type: "object",
    properties: { region: { type: "string", "x-mcp-header": "Region" } },
    required: ["region"],
  },
  outputSchema: { type: "object", properties: { answer: { type: "integer" } }, required: ["answer"] },
};
const server = createServer(async (request, response) => {
  const path = request.url.slice(1);
  if (path === "reset") {
    requests.clear(); closed.clear(); started.clear(); records.length = 0; effects = 0;
    response.end("reset"); return;
  }
  if (path === "stats") {
    response.setHeader("content-type", "application/json");
    response.end(JSON.stringify({ requests: Object.fromEntries(requests), closed: [...closed], started: [...started], effects, records })); return;
  }
  requests.set(path, (requests.get(path) ?? 0) + 1);
  response.on("close", () => closed.add(path));
  try {
    const chunks = [];
    for await (const chunk of request) chunks.push(chunk);
    const body = JSON.parse(Buffer.concat(chunks));
    assert.equal(request.method, "POST");
    assert.equal(request.headers.authorization, "Bearer conformance-only");
    assert.equal(request.headers.accept, "application/json");
    assert.equal(request.headers["mcp-session-id"], undefined);
    assert.equal(request.headers["mcp-method"], body.method);
    assert.equal(request.headers["mcp-protocol-version"], "2026-07-28");
    assert.equal(body.params._meta["io.modelcontextprotocol/protocolVersion"], "2026-07-28");
    assert.deepEqual(body.params._meta["io.modelcontextprotocol/clientCapabilities"], {});
    const client = body.params._meta["io.modelcontextprotocol/clientInfo"].name;
    assert.ok(["worker-conformance", "native-example"].includes(client));
    const record = { at: new Date().toISOString(), path, client, id: body.id, method: body.method, arguments: body.params.arguments, protocol: request.headers["mcp-protocol-version"], regionHeader: request.headers["mcp-param-region"] };
    records.push(record);
    const send = (result, status = 200, id = body.id) => {
      record.status = status;
      record.result = result;
      response.writeHead(status, { "content-type": "application/json" });
      response.end(JSON.stringify({ jsonrpc: "2.0", id, result }));
    };
    if (body.method === "tools/list") {
      const second = body.params.cursor === "second";
      send({ resultType: "complete", ttlMs: 0, cacheScope: "private", tools: [{ ...definition, name: second ? "other" : "lookup" }], ...(second ? {} : { nextCursor: "second" }) });
      return;
    }
    assert.equal(body.method, "tools/call");
    assert.equal(request.headers["mcp-name"], body.params.name);
    assert.equal(request.headers["mcp-param-region"], body.params.arguments.region);
    effects++;
    switch (path) {
      case "disconnect": request.socket.destroy(); return;
      case "timeout":
      case "sse":
      case "http-html":
      case "http-timeout":
      case "cancel-headers":
      case "cancel-body": {
        if (path !== "cancel-headers") {
          const media = path === "sse" ? "text/event-stream" : path === "http-html" ? "text/html" : "application/json";
          response.writeHead(path.startsWith("http-") ? 503 : 200, { "content-type": media });
          response.flushHeaders();
          if (media === "application/json") response.write("{");
        }
        started.add(path);
        const timer = setTimeout(() => response.end(), 10_000);
        response.on("close", () => clearTimeout(timer));
        return;
      }
      case "oversize":
      case "http-oversize":
        response.writeHead(path === "oversize" ? 200 : 503, { "content-type": "application/json" });
        response.write(" ".repeat(8192)); response.end(); return;
      case "http-empty": response.writeHead(401); response.end(); return;
      case "http-json":
        response.writeHead(429, { "content-type": "application/json" });
        response.end(JSON.stringify({ message: "private-marker" })); return;
      case "http-malformed":
        response.writeHead(502, { "content-type": "application/json" });
        response.end("{"); return;
      case "http-missing-id":
      case "http-wrong-id":
        response.writeHead(403, { "content-type": "application/json" });
        response.end(JSON.stringify({ jsonrpc: "2.0", ...(path === "http-wrong-id" ? { id: "wrong" } : {}), error: { code: -32022, message: "private-marker" } })); return;
      case "redirect":
        response.writeHead(307, { "content-type": "application/json", location: "/followed" });
        response.end(JSON.stringify({ jsonrpc: "2.0", id: body.id, result: { resultType: "complete", content: [] } })); return;
      case "rpc":
        response.writeHead(400, { "content-type": "application/json" });
        response.end(JSON.stringify({ jsonrpc: "2.0", id: body.id, error: { code: -32022, message: "private-marker" } })); return;
      default:
        const answer = path === "bad-schema" ? "wrong" : body.params.arguments.region === "east" ? 12 : 37;
        send({
          resultType: "complete",
          isError: path === "is-error",
          content: [{ type: "text", text: JSON.stringify({ answer }, null, 2), _meta: { secret: "private-marker" } }],
          structuredContent: { answer },
          _meta: { secret: "private-marker" },
        }, 200, path === "mismatch" ? "wrong" : body.id);
    }
  } catch (error) {
    console.error(error);
    response.writeHead(500, { "content-type": "application/json" });
    response.end(JSON.stringify({ fixtureError: error.message }));
  }
});
server.listen(9899, "127.0.0.1", () => console.log("MCP fixture ready"));
