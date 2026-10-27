#![cfg(unix)]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use rig_worker_tests::{Service, command, free_port, http};
use serde_json::{Value, json};

fn limits() -> Value {
    json!({
        "source_bytes": 65536, "memory_bytes": 8 * 1024 * 1024,
        "wall_time": {"secs": 5, "nanos": 0}, "max_calls": 64,
        "max_in_flight": 8, "message_bytes": 65536, "output_bytes": 65536
    })
}

fn budget(bytes: u64) -> Value {
    json!({"bytes": bytes, "work": 1000000, "elapsed": {"secs": 5, "nanos": 0}})
}

fn request(code: &str) -> Value {
    let catalog: Vec<_> = ["echo", "other"]
        .into_iter()
        .map(|name| {
            json!({"name": name, "description": "echo value",
                "input_schema": {"type": "object"}, "presentation": "inline"})
        })
        .collect();
    json!({"version": 1, "code": code, "catalog": catalog, "limits": limits(),
        "prefix": [], "budget": budget(16 * 1024 * 1024)})
}

fn suffix(response: Value) -> Value {
    response["batches"]
        .as_array()
        .unwrap()
        .last()
        .unwrap()
        .clone()
}

fn returned(done: &Value) -> Value {
    serde_json::from_str(done["returned"].as_str().unwrap()).unwrap()
}

fn resolve(input: &mut Value, batch: Value, ordinal: u64, payload: &str) {
    input["prefix"].as_array_mut().unwrap().push(json!({
        "batch": batch, "reply": {"ordinal": ordinal, "payload": {"Resolve": payload}}
    }));
}

#[test]
#[ignore = "requires Node and npm ci in rig-codemode/runtime"]
fn dynamic_worker_replay_conformance() {
    let port = free_port();
    let url = format!("http://127.0.0.1:{port}/exchange");
    let temporary = tempfile::tempdir().unwrap();
    let mut adapter = command("node");
    adapter.env("TMPDIR", temporary.path());
    adapter.args(["rig-codemode/runtime/miniflare.mjs", &port.to_string()]);
    let mut service = Service::start(adapter);
    service.wait_http(&format!("http://127.0.0.1:{port}/health"));
    let client = http();
    let send = |input: &Value, expected: u16| -> Value {
        let response = client
            .post(&url)
            .json(input)
            .send()
            .unwrap_or_else(|error| panic!("exchange failed for {input}: {error:?}"));
        let status = response.status().as_u16();
        let text = response.text().unwrap();
        assert_eq!(status, expected, "{text}");
        if expected == 200 {
            serde_json::from_str(&text).unwrap()
        } else {
            Value::String(text)
        }
    };

    // 1. Drain deep promises and retain call ordering across out-of-order replies.
    {
        let mut input = request(
            "let p = Promise.resolve(0); for(let i=0;i<300;i++) p=p.then(n=>n+1); text(await p); return await Promise.all([tools.echo({n:1}),tools.other({n:2})]);",
        );
        let first = suffix(send(&input, 200));
        let names: Vec<_> = first["events"]
            .as_array()
            .unwrap()
            .iter()
            .map(|event| event["Call"]["name"].clone())
            .collect();
        assert_eq!(names, vec![json!("echo"), json!("other")], "{first}");
        assert_eq!(first["boundary"]["Idle"]["output"]["text"], "300\n");
        resolve(&mut input, first, 1, "\"second\"");
        let second = suffix(send(&input, 200));
        assert_eq!(second["events"], json!([]));
        resolve(&mut input, second, 0, "\"first\"");
        let terminal = suffix(send(&input, 200))["boundary"]["Done"].clone();
        assert_eq!(terminal["output"]["text"], "300\n");
        assert_eq!(returned(&terminal), json!(["first", "second"]));
    }
    println!("PASS deep promise drain and ordered parallel calls");

    // 2. Inject one reply, then drain its entire continuation before returning.
    {
        let mut input = request(
            "const winner=await Promise.race([tools.echo({n:1}),tools.other({n:2})]); await Promise.resolve().then(()=>Promise.resolve()); return await tools.echo({winner});",
        );
        let first = suffix(send(&input, 200));
        resolve(&mut input, first, 1, "\"other-wins\"");
        let next = suffix(send(&input, 200));
        assert_eq!(
            next["events"],
            json!([{"Call": {"ordinal": 2, "name": "echo",
            "arguments": "{\"winner\":\"other-wins\"}", "raw": false}}])
        );
    }
    println!("PASS one reply before full continuation drain");

    // 3. Every tampered prefix is a negative control, including changed code.
    {
        let input = request("text('once'); return await tools.echo({n:7});");
        let batch = suffix(send(&input, 200));
        for mutation in 0..4 {
            let mut prefix = json!({"batch": batch,
                "reply": {"ordinal": 0, "payload": {"Resolve": "7"}}});
            match mutation {
                0 => prefix["batch"]["events"][0]["Call"]["arguments"] = json!("{\"n\":8}"),
                1 => prefix["batch"]["boundary"]["Idle"]["output"]["text"] = json!("twice\n"),
                2 => prefix["batch"]["boundary"] = json!({"Done": {}}),
                3 => prefix["reply"]["ordinal"] = json!(99),
                _ => unreachable!(),
            }
            let mut tampered = input.clone();
            tampered["prefix"] = json!([prefix]);
            send(&tampered, 422);
        }
        let mut changed_code = input;
        changed_code["code"] = json!("return 1");
        resolve(&mut changed_code, batch, 0, "7");
        send(&changed_code, 422);
    }
    println!("PASS tampered prefix arguments, output, boundary and unknown targets reject");

    // 4. Guest mutation may change guest JSON but not trusted protocol state.
    {
        let input = request(
            "globalThis.secret=99; JSON.stringify=()=> '\"forged\"'; Object.prototype.toJSON=()=> 'forged'; return await tools.echo({n:3});",
        );
        let first = suffix(send(&input, 200));
        assert_eq!(first["events"][0]["Call"]["arguments"], "\"forged\"");
        assert_eq!(first["events"][0]["Call"]["name"], "echo");
        assert_eq!(
            first["boundary"]["Idle"]["output"],
            json!({"text": "", "truncated": false})
        );
        let terminal = suffix(send(&request("return typeof secret"), 200));
        assert_eq!(returned(&terminal["boundary"]["Done"]), "undefined");
        let clean = suffix(send(&request("return await tools.echo({n:3})"), 200));
        assert_eq!(clean["events"][0]["Call"]["arguments"], "{\"n\":3}");
    }
    println!("PASS fresh state and prototype mutations do not corrupt trusted events");

    // 5. Byte bounds preserve UTF-8 boundaries and reject invalid output/returns.
    {
        let mut input = request("text('héllo'); text('more');");
        input["limits"]["output_bytes"] = json!(2);
        let terminal = suffix(send(&input, 200));
        assert_eq!(
            terminal["boundary"]["Done"]["output"],
            json!({"text": "h", "truncated": true})
        );
        input["code"] = json!("text({n:9})");
        assert_eq!(
            suffix(send(&input, 200))["boundary"]["Done"]["status"],
            "script_error"
        );
        let mut input = request("return 'x'.repeat(100)");
        input["limits"]["message_bytes"] = json!(32);
        assert_eq!(
            suffix(send(&input, 200))["boundary"]["Done"]["status"],
            "script_error"
        );
    }
    println!("PASS output unicode boundary and non-string bounds");

    // 6. NUL bytes survive text, discovery, and rejected replies without truncation.
    {
        let done = suffix(send(
            &request(r"text('a\0b');return [describeTool('echo\0junk'),searchTools('echo\0')];"),
            200,
        ));
        assert_eq!(done["boundary"]["Done"]["output"]["text"], "a\0b\n");
        let result = returned(&done["boundary"]["Done"]);
        assert_eq!(result[0], Value::Null);
        assert_eq!(result[1][0]["name"], "echo");
        let query = suffix(send(
            &request(r"try{searchTools('echo\0'+'x'.repeat(4096))}catch(e){return e.message}"),
            200,
        ));
        assert!(
            returned(&query["boundary"]["Done"])
                .as_str()
                .unwrap()
                .contains("4096")
        );
        let mut input = request("try{await tools.echo({})}catch(e){return [e.message,e.tool]}");
        let batch = suffix(send(&input, 200));
        input["prefix"] = json!([{"batch": batch, "reply": {"ordinal": 0,
            "payload": {"Reject": {"message": "a\0b", "kind": "other", "status": "error", "tool": "echo\0suffix"}}}}]);
        assert_eq!(
            returned(&suffix(send(&input, 200))["boundary"]["Done"]),
            json!(["a\0b", "echo\0suffix"])
        );
        for code in [r"throw '\uD800'", r"throw new Error('\uD800')"] {
            let result = suffix(send(&request(code), 200));
            assert_eq!(result["boundary"]["Done"]["status"], "script_error");
            assert_eq!(result["boundary"]["Done"]["diagnostic"]["message"], "error");
        }
        for expression in [r"searchTools('\uD800')", r"describeTool('\uD800')"] {
            let result = suffix(send(
                &request(&format!(
                    "try {{ {expression}; return false }} catch(e) {{ return true }}"
                )),
                200,
            ));
            assert_eq!(returned(&result["boundary"]["Done"]), true);
        }
    }
    println!("PASS embedded NUL text, exact discovery names and rejection messages");

    // 7. Fatal transport exhaustion cannot be caught by guest JavaScript.
    {
        let mut input = request(
            "try{await Promise.all(Array.from({length:16},()=>tools.echo({s:'x'.repeat(1024)})))}catch(e){};return 1",
        );
        input["budget"] = budget(5000);
        send(&input, 429);
        let mut input = request(r"try{await tools.echo({s:'\0'.repeat(128)})}catch(e){};return 1");
        input["budget"] = budget(1000);
        send(&input, 429);
        let mut input = request("text('x'.repeat(512));await tools.echo({});await tools.echo({});");
        input["budget"] = budget(1200);
        let batch = suffix(send(&input, 200));
        resolve(&mut input, batch, 0, "null");
        send(&input, 429);
        assert_eq!(
            returned(&suffix(send(&request("return 'fresh'"), 200))["boundary"]["Done"]),
            "fresh"
        );
    }
    println!("PASS aggregate events, escaping and repeated snapshots exhaust transport fatally");

    // 8. No host capabilities leak through eval or constructors; tools stay frozen.
    {
        let terminal = suffix(send(
            &request(
                "return [typeof fetch,typeof WebSocket,typeof process,typeof require,typeof caches,typeof crypto,typeof Worker,typeof __host,typeof Date,typeof WeakRef,typeof FinalizationRegistry,Function('return typeof fetch')(),eval('typeof process')];",
            ),
            200,
        ));
        assert_eq!(
            returned(&terminal["boundary"]["Done"]),
            json!(vec!["undefined"; 13])
        );
        assert_eq!(
            suffix(send(&request("Math.random()"), 200))["boundary"]["Done"]["status"],
            "script_error"
        );
        let frozen = suffix(send(
            &request(
                "return [Object.getPrototypeOf(tools)===null,Object.isFrozen(tools),Object.isFrozen(tools.echo),Object.isFrozen(tools.echo.raw)]",
            ),
            200,
        ));
        assert_eq!(
            returned(&frozen["boundary"]["Done"]),
            json!([true, true, true, true])
        );
    }
    println!("PASS capabilities absent, constructors and eval stay in QuickJS");

    // 9. Exercise every runaway workload, not just a CPU loop.
    {
        for code in [
            "while(true){}",
            "await new Promise(()=>{const storm=()=>Promise.resolve().then(storm);storm()})",
            "const a=[];while(true)a.push('x'.repeat(65536));",
            "function recurse(){return recurse()} recurse();",
        ] {
            println!("CHECK bounded guest: {code}");
            let result = send(&request(code), 200);
            let batch = suffix(result.clone());
            let status = batch["boundary"]["Done"]["status"].as_str().unwrap();
            assert!(["timed_out", "script_error"].contains(&status), "{result}");
            assert!(result["usage"]["work"].as_u64().unwrap() > 0, "{result}");
        }
    }
    println!("PASS CPU loop, promise job storm, heap exhaustion and stack overflow stop");

    // 10. Serialization rejection remains catchable; raw calls and budgets survive replay.
    {
        let result = suffix(send(
            &request(
                "const cycle={};cycle.self=cycle; try{await tools.echo(cycle)}catch(e){text(e.kind)} return await tools.other.raw({n:8});",
            ),
            200,
        ));
        assert_eq!(result["events"][0]["Rejected"]["kind"], "invalid_args");
        assert_eq!(result["events"][1]["Call"]["raw"], true);
        assert_eq!(
            result["boundary"]["Idle"]["output"]["text"],
            "invalid_args\n"
        );
        let mut input =
            request("await tools.echo({}); try{await tools.echo({})}catch(e){return e.kind}");
        input["limits"]["max_calls"] = json!(1);
        input["limits"]["max_in_flight"] = json!(1);
        let first = suffix(send(&input, 200));
        resolve(&mut input, first, 0, "1");
        let terminal = suffix(send(&input, 200));
        assert_eq!(returned(&terminal["boundary"]["Done"]), "other");
        assert_eq!(terminal["boundary"]["Done"]["over_budget_calls"], 1);
    }
    println!("PASS serialization rejection, raw call and call budget");

    // 11. Unsafe wire integers and minimal transport budgets fail with distinct statuses.
    {
        let mut input = request("return 1");
        input["budget"] = json!({"bytes": 1000000, "work": 1_u64 << 53,
            "elapsed": {"secs": 1, "nanos": 0}});
        send(&input, 422);
        input["budget"] = json!({"bytes": 1, "work": 1000,
            "elapsed": {"secs": 1, "nanos": 0}});
        send(&input, 429);
    }
    println!("PASS unsafe integers and transport exhaustion reject");
    println!("11 real Dynamic Worker conformance groups passed");
    drop(service);
    assert!(
        std::fs::read_dir(temporary.path())
            .unwrap()
            .next()
            .is_none(),
        "Miniflare must remove its temporary resources on shutdown"
    );
}

#[test]
#[ignore = "requires Node with VM modules and built replay assets"]
fn bridge_budgets_bound_wasm_memory() {
    let mut input = request("return Array(1200000).fill(0)");
    input["limits"]["memory_bytes"] = json!(64 * 1024 * 1024);
    input["limits"]["message_bytes"] = json!(4 * 1024 * 1024);
    input["limits"]["output_bytes"] = json!(101);
    input["budget"] = budget(4096);
    let result = command("node")
        .args(["--experimental-vm-modules", "--input-type=module", "-e", r#"
import { readFile } from 'node:fs/promises';
import { createContext, SourceTextModule, SyntheticModule } from 'node:vm';
const path = 'rig-codemode/runtime/';
const source = await readFile(path + 'worker.js', 'utf8');
const wasm = await WebAssembly.compile(await readFile(path + 'quickjs.wasm'));
const input = JSON.parse(process.argv[1]);
const results = [];
for (const code of [input.code, 'const a=Array(1200000).fill(0); return 0',
  "throw 'x'.repeat(24 * 1024 * 1024)",
  "const s='x'.repeat(24 * 1024 * 1024); throw 'control'"]) {
  let memory;
  const context = createContext({ TextEncoder, TextDecoder, Response,
    WebAssembly: { Instance: class extends WebAssembly.Instance {
      constructor(...args) { super(...args); memory = this.exports.memory; }
    } },
  });
  const worker = new SourceTextModule(source, { context });
  await worker.link((name) => {
    if (name !== './quickjs.wasm') throw Error('unexpected import: ' + name);
    return new SyntheticModule(['default'], function() { this.setExport('default', wasm); }, { context });
  });
  await worker.evaluate();
  const response = await worker.namespace.default.fetch(new Request('http://test/', {
    method: 'POST', body: JSON.stringify({ ...input, code }),
  }));
  results.push({ status: response.status, memory: memory.buffer.byteLength, body: await response.text() });
}
console.log(JSON.stringify(results));
"#])
        .arg(input.to_string())
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let results: Value = serde_json::from_slice(&result.stdout).unwrap();
    assert_eq!(results[0]["status"], 429, "{results}");
    assert_eq!(results[1]["status"], 200, "{results}");
    let control: Value = serde_json::from_str(results[1]["body"].as_str().unwrap()).unwrap();
    assert_eq!(returned(&suffix(control)["boundary"]["Done"]), 0);
    for (index, message) in [(2, "error"), (3, "control")] {
        assert_eq!(results[index]["status"], 200, "{results}");
        let body: Value = serde_json::from_str(results[index]["body"].as_str().unwrap()).unwrap();
        let batch = suffix(body);
        assert_eq!(batch["boundary"]["Done"]["status"], "script_error");
        assert_eq!(batch["boundary"]["Done"]["diagnostic"]["message"], message);
    }
    for result in results.as_array().unwrap() {
        assert!(
            result["memory"].as_u64().unwrap() < 40 * 1024 * 1024,
            "{results}"
        );
    }
    println!(
        "PASS bridge memory: return={} control={} diagnostic={} control={}",
        results[0]["memory"], results[1]["memory"], results[2]["memory"], results[3]["memory"]
    );
}
