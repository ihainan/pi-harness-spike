// ACP server entry: one process, many sessions, one stdio pipe.
import { randomUUID } from "node:crypto";
import { AcpPeer, RpcError } from "./acp.js";
import { fault } from "./fault.js";
import { Fence } from "./fence.js";
import { connectMcp } from "./mcp.js";
import { loadConfig } from "./model.js";
import { Session } from "./session.js";

// stdout carries ACP frames only; a stray console.log would corrupt the protocol.
console.log = console.error;
console.info = console.error;
console.warn = console.error;
console.debug = console.error;

const cfg = loadConfig();
// Nothing below touches session files until this process owns the state directory.
Session.fence = await Fence.acquire(cfg.stateDir);
const mcpReady = connectMcp(cfg.mcpConfigPath);
const sessions = new Map<string, Session>();
const peer = new AcpPeer();
const sendUpdate = (sessionId: string, update: Record<string, unknown>) => peer.notify("session/update", { sessionId, update });

function promptText(params: any): string {
	return (params.prompt ?? []).filter((b: any) => b?.type === "text").map((b: any) => b.text).join("\n");
}

peer.on("initialize", async () => {
	await mcpReady;
	return {
		protocolVersion: 1,
		agentCapabilities: { loadSession: !fault("no_loadsession"), promptCapabilities: { image: false, audio: false, embeddedContext: false } },
		authMethods: [],
		agentInfo: { name: "pi-harness-spike", version: "0.0.1" },
	};
});

peer.on("session/new", async (params) => {
	const mcp = await mcpReady;
	const id = randomUUID();
	sessions.set(id, new Session(id, params.cwd ?? process.cwd(), cfg, mcp, sendUpdate));
	return { sessionId: id };
});

peer.on("session/load", async (params) => {
	const mcp = await mcpReady;
	const id = String(params.sessionId);
	const live = sessions.get(id);
	if (live) return {};
	const hist = Session.openStored(cfg, id);
	if (!hist && fault("silent_load")) {
		sessions.set(id, new Session(id, params.cwd ?? process.cwd(), cfg, mcp, sendUpdate));
		return {};
	}
	if (!hist) throw new RpcError(-32002, `session not found: ${id}`);
	const session = new Session(id, params.cwd ?? hist.cwd ?? process.cwd(), cfg, mcp, sendUpdate, hist.messages, hist.log);
	session.restore(hist.notes, hist.stored);
	sessions.set(id, session);
	session.replayHistory(hist.messages);
	return {};
});

peer.on("session/prompt", async (params) => {
	const session = sessions.get(String(params.sessionId));
	if (!session) throw new RpcError(-32002, `session not found: ${params.sessionId}`);
	const text = promptText(params);
	const meta = params._meta ?? {};
	if (text.startsWith("/steer ")) {
		session.steer(text.slice("/steer ".length), params.steerExpectedTurn ?? meta.zgSteerExpectedTurn);
		return { stopReason: "end_turn", _meta: { harness: { steerAccepted: true } } };
	}
	return session.prompt(text, params.turnToken ?? meta.zgTurnToken);
});

peer.on("session/cancel", (params) => {
	sessions.get(String(params.sessionId))?.cancel();
});

function shutdown(code: number): void {
	for (const s of sessions.values()) s.dispose();
	mcpReady.then((m) => m.close()).finally(() => process.exit(code));
	setTimeout(() => process.exit(code), 1500).unref();
}
process.on("SIGTERM", () => shutdown(0));
process.on("SIGINT", () => shutdown(0));
peer.listen(() => shutdown(0));
