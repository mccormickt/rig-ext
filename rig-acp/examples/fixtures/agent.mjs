#!/usr/bin/env node
// Deterministic ACP v1 peer. This is a protocol fixture, not a coding agent.
import { createInterface } from 'node:readline';
import { appendFileSync } from 'node:fs';

const sessions = new Map();
const permissions = new Map();
let permissionId = 1000;
const send = value => process.stdout.write(`${JSON.stringify({ jsonrpc: '2.0', ...value })}\n`);
const result = (id, value) => send({ id, result: value });
const update = (sessionId, text, kind = 'agent_message_chunk') => send({
  method: 'session/update', params: { sessionId, update: {
    sessionUpdate: kind, content: { type: 'text', text },
  } },
});
const done = (id, stopReason = 'end_turn') => result(id, { stopReason });
const receipt = value => {
  if (process.env.ACP_FIXTURE_RECEIPTS) appendFileSync(process.env.ACP_FIXTURE_RECEIPTS,
    `${JSON.stringify({ time: new Date().toISOString(), pid: process.pid, ...value })}\n`);
};

createInterface({ input: process.stdin }).on('line', line => {
  const message = JSON.parse(line);
  const { id, method, params } = message;
  receipt({ direction: 'in', id, method, sessionId: params?.sessionId });
  if (!method) {
    const pending = permissions.get(id);
    if (!pending) return;
    permissions.delete(id);
    const state = sessions.get(pending.sessionId);
    const allowed = message.result?.outcome?.optionId === 'allow-once';
    if (allowed) state.effects++;
    receipt({ event: 'permission-effect', id, sessionId: pending.sessionId, allowed, effects: state.effects });
    update(pending.sessionId, allowed ? 'approved' : 'denied');
    done(pending.promptId);
    return;
  }
  if (method === 'initialize') {
    receipt({ event: 'initialize', capabilities: params.clientCapabilities });
    if (process.env.ACP_HOLD_INITIALIZE) return;
    result(id, { protocolVersion: process.env.ACP_BAD_VERSION ? 99 : 1,
      agentCapabilities: {}, agentInfo: { name: 'rig-acp-fixture', version: '1' }, authMethods: [] });
  } else if (method === 'session/new') {
    const sessionId = `fixture-${sessions.size + 1}`;
    sessions.set(sessionId, { effects: 0, cancels: 0, turns: 0, remembered: 'empty', wait: null });
    result(id, { sessionId });
  } else if (method === 'session/cancel') {
    const state = sessions.get(params.sessionId);
    state.cancels++;
    if (state.wait !== null) {
      const promptId = state.wait;
      state.wait = null;
      receipt({ event: 'cancel-settled', sessionId: params.sessionId, effects: state.effects });
      done(promptId, 'cancelled');
    }
  } else if (method === 'session/prompt') {
    const sessionId = params.sessionId;
    const state = sessions.get(sessionId);
    if (!state) { send({ id, error: { code: -32602, message: 'unknown session' } }); return; }
    const text = params.prompt.map(block => block.text || '').join('\n');
    state.turns++;
    receipt({ event: 'prompt', id, sessionId, text, turns: state.turns });
    if (text === 'permission') {
      update(sessionId, 'before:');
      const requestId = permissionId++;
      permissions.set(requestId, { sessionId, promptId: id });
      send({ id: requestId, method: 'session/request_permission', params: {
        sessionId, toolCall: { toolCallId: `effect-${requestId}`, title: 'Increment fixture effect counter', status: 'pending' },
        options: [
          { optionId: 'deny-once', name: 'Deny', kind: 'reject_once' },
          { optionId: 'allow-once', name: 'Allow', kind: 'allow_once' },
        ],
      } });
      update(sessionId, 'during:');
    } else if (text === 'wait') {
      state.wait = id;
      update(sessionId, 'effect-gated');
    } else if (text === 'release') {
      for (const [otherId, other] of sessions) if (other.wait !== null) {
        other.effects++;
        done(other.wait);
        other.wait = null;
        receipt({ event: 'released-effect', sessionId: otherId, effects: other.effects });
      }
      update(sessionId, 'released'); done(id);
    } else if (text === 'stats') {
      update(sessionId, JSON.stringify({ effects: state.effects, cancels: state.cancels, turns: state.turns })); done(id);
    } else if (text === 'disconnect-after-effect') {
      state.effects++;
      receipt({ event: 'ambiguous-effect', id, sessionId, effects: state.effects });
      process.exit(0);
    } else if (text === 'error') {
      send({ id, error: { code: -32603, message: 'fixture failure' } });
    } else if (text === 'malformed') {
      process.stdout.write('{not-json}\n'); process.exit(0);
    } else if (text === 'flood') {
      for (let n = 0; n < 130; n++) update(sessionId, 'x'.repeat(65536));
      done(id);
    } else {
      if (text.startsWith('remember ')) state.remembered = text.slice(9);
      const answer = text === 'recall' ? state.remembered : text.startsWith('remember ') ? 'remembered' : `echo:${text}`;
      update(sessionId, 'not-projected', 'agent_thought_chunk');
      update(sessionId, answer.slice(0, 2));
      update(sessionId, answer.slice(2));
      done(id);
    }
  } else if (id !== undefined) {
    send({ id, error: { code: -32601, message: 'unsupported method' } });
  }
});
