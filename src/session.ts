// One ACP session = one pi Agent + one JSONL file. Maps agent events to ACP updates, and owns
// steer, cancel, overflow recovery and persistence.
import { randomUUID } from "node:crypto";
import * as fs from "node:fs";
import * as path from "node:path";
import { Agent, type AgentEvent, type AgentMessage } from "@earendil-works/pi-agent-core";
import { isContextOverflow } from "@earendil-works/pi-ai";
import { RpcError } from "./acp.js";
import { compact, estimateContext } from "./compaction.js";
import type { McpHub } from "./mcp.js";
import type { HarnessConfig } from "./model.js";
import { fault } from "./fault.js";
import { InlineThinkSplitter, stripInlineThink, ToolLoopGuard } from "./guards.js";
import { type Fence, FencedError } from "./fence.js";
import { knownResults, lateMessage, type LateResult, OpRegistry, placeLiveNote, reconcileOps } from "./ops.js";
import { repairTranscript, replayEntries, SessionCorruptError, SessionLog } from "./sessionlog.js";
import { makeBuiltinTools, ProcessTable } from "./tools.js";

const SYSTEM_PROMPT = process.env.PI_SYSTEM_PROMPT_FILE ? fs.readFileSync(process.env.PI_SYSTEM_PROMPT_FILE, "utf8") :
	"You are a helpful assistant running inside a workplace agent platform. Use the available tools when asked. " +
	"Follow the user's instructions exactly, and reply in the user's language.";
const STEER_DELIVERY = "\x1fHARNESS_STEER_DELIVERY\x1f";
// A stream that keeps sending thinking deltas never trips an idle timeout, so this is the only limit on it. It is a last
// line, not a judgement of slowness: across 1,683 real requests the first output came as late as 851 s and was good (53K
// tokens), while the real runaway ran 996 s. 300 s here killed all reruns of a task that normally scores. Most runaways
// end by themselves when the output budget runs out, and the empty-answer path below catches those.
const THINK_MAX_MS = Number(process.env.PI_THINK_MAX_MS ?? 900_000);
const THINK_RETRIES = 1;
// A broken stream or a gateway hiccup. 29 of 1,799 real requests ended without a finish reason. Retrying the model call is
// safe: tools only run once an answer is complete, and this answer never was. Overflow is handled before this.
const TRANSIENT_ERROR = /without finish_reason|stream|terminated|socket|econn|etimedout|timeout|fetch failed|network|overloaded|rate limit|\b(429|500|502|503|504|529)\b/i;
const TRANSIENT_RETRIES = 2;
const TRANSIENT_BACKOFF_MS = Number(process.env.PI_TRANSIENT_BACKOFF_MS ?? 2000);
const CROWDED_RATIO = 0.7;

/** The model answered with nothing: no text and no tool call. Normal-looking stop reasons hide this. */
function producedNothing(m: any): boolean {
	if (m?.role !== "assistant") return false;
	return !(m.content ?? []).some((b: any) => (b.type === "text" && b.text?.trim()) || b.type === "toolCall");
}

export type SendUpdate = (sessionId: string, update: Record<string, unknown>) => void;

function titleFor(toolName: string, args: any): string {
	const summary = args?.command ?? args?.path ?? args?.handle ?? "";
	return summary ? `${toolName}: ${String(summary).slice(0, 120)}` : toolName;
}

function kindFor(toolName: string): string {
	if (toolName === "terminal" || toolName === "process") return "execute";
	if (toolName === "read_file") return "read";
	if (toolName === "write_file") return "edit";
	return "other";
}

function resultText(result: any): string {
	return (result?.content ?? []).map((b: any) => (b.type === "text" ? b.text : "")).join("");
}

export class Session {
	static fence?: Fence; // Set once at startup; every session log in the process shares it.
	static lastPrompt = ""; // only for the cross_session_leak negative control
	static peers: Session[] = []; // only for the op_late_leak negative control
	readonly agent: Agent;
	readonly table = new ProcessTable();
	private log: SessionLog;
	private persistError?: Error;
	private ops: OpRegistry;
	private pendingLate: any[] = [];
	// In the Agent's queue, not yet in the transcript, in the order they arrived: steers and late results can depend on it.
	private queued = new Map<string, any>();
	private deliveredLate: string[] = []; // In the transcript, but no answer has been produced from them yet.
	private turnHadEffects = false; // A tool ran or a steer was accepted, so this turn cannot simply be taken back.
	private sawOutput = false;
	private thinkTimedOut = false;
	private thinkTimer?: ReturnType<typeof setTimeout>;
	private thinkCtl?: AbortController;
	private thinkStart = 0;
	private turnAbort = new AbortController();
	private turnToken?: string;
	private running = false;
	private cancelled = false;
	private pendingSteers: string[] = [];
	private pendingSteerIds = new Map<string, string>();
	private lastUsed = 0;
	private compactions = 0;
	private splitter = new InlineThinkSplitter();
	private guard = new ToolLoopGuard();

	constructor(
		readonly id: string,
		readonly cwd: string,
		private cfg: HarnessConfig,
		mcp: McpHub,
		private send: SendUpdate,
		history: AgentMessage[] = [],
		log?: SessionLog,
	) {
		fs.mkdirSync(path.join(cfg.stateDir, "sessions"), { recursive: true });
		fs.mkdirSync(cwd, { recursive: true });
		this.log = log ?? SessionLog.open(Session.fileFor(cfg, id), Session.logOptions());
		if (!log) this.appendEntry({ t: "hdr", id, cwd, created: Date.now() });
		Session.peers.push(this);
		this.ops = new OpRegistry(`op-${id.slice(0, 8)}-${Date.now().toString(36)}`, {
			persist: (e) => this.appendStrict(e),
			onLate: (late) => this.onLate(late),
			steerPending: () => this.pendingSteers.length > 0,
		}, cfg.mcpTimeoutMs);
		this.agent = new Agent({
			initialState: {
				systemPrompt: SYSTEM_PROMPT,
				model: cfg.model,
				thinkingLevel: cfg.thinkingLevel,
				tools: [...makeBuiltinTools(cwd, this.table), ...mcp.wrapTools(id, this.ops)],
				messages: history,
			},
			// Agent has no maxRetries option; the stream options are the only way in. 429/5xx would otherwise end the turn.
			streamFn: ((model: any, context: any, options: any) => {
				const ctl = new AbortController();
				const upstream: AbortSignal | undefined = options?.signal;
				if (upstream?.aborted) ctl.abort();
				else upstream?.addEventListener("abort", () => ctl.abort(), { once: true });
				this.sawOutput = false;
				this.thinkTimedOut = false;
				this.thinkCtl = ctl;
				this.thinkStart = Date.now();
				this.armThinkWatchdog();
				return cfg.models.streamSimple(model, context, { ...options, signal: ctl.signal, maxRetries: Number(process.env.PI_MAX_RETRIES ?? 2) });
			}) as any,
			finishTurn: (turn) => this.guard.check(turn),
			sessionId: id,
			// Default is one-at-a-time; platform may send several steers and wants them seen together.
			steeringMode: "all",
			prepareRequest: async (req, signal) => {
				if (fault("no_compact")) return undefined;
				// A failed summary must not kill the turn; the provider's overflow error is the backstop.
				// A context returned here stays with the loop, so last round's note must go before anything else looks at it.
				const clean = req.context.messages.filter((m: any) => !m.zgLiveNote);
				const hadNote = clean.length !== req.context.messages.length;
				const res = await this.maybeCompact(clean, signal, false).catch((err) => {
					console.error(`[compaction] failed: ${err?.message ?? err}`);
					return undefined;
				});
				// Not stored: it describes this moment, and compaction may have removed the placeholder that said the same.
				const live = this.ops.liveNote();
				if (!res && !live && !hadNote) return undefined;
				const messages = res ?? clean;
				if (!live) return { context: { ...req.context, messages } };
				const note = { role: "user", content: [{ type: "text", text: live }], timestamp: Date.now(), zgLiveNote: true } as any;
				return { context: { ...req.context, messages: placeLiveNote(messages, note) } };
			},
		});
		this.table.pollOp = (handle, action) => this.ops.poll(handle, action);
		this.table.steerPending = () => this.pendingSteers.length > 0; // A late op result must not push a foreground command to the background.
		this.agent.subscribe((ev) => this.onEvent(ev));
	}

	private static logOptions() {
		return { guard: () => Session.fence?.check(), generation: Session.fence?.generation };
	}

	static fileFor(cfg: HarnessConfig, id: string): string {
		if (!/^[A-Za-z0-9_-]+$/.test(id)) throw new RpcError(-32602, `invalid session id: ${id}`);
		return path.join(cfg.stateDir, "sessions", `${id}.jsonl`);
	}

	/** Opens a stored session, or returns undefined if there is none. Damage is an explicit error, never an empty session. */
	static openStored(cfg: HarnessConfig, id: string): { log: SessionLog; cwd?: string; messages: AgentMessage[]; notes: any[]; stored: any[] } | undefined {
		const file = Session.fileFor(cfg, id);
		if (!SessionLog.exists(file)) return undefined;
		let log: SessionLog;
		try {
			log = SessionLog.open(file, Session.logOptions());
		} catch (err) {
			if (err instanceof SessionCorruptError) throw new RpcError(-32003, err.message);
			throw err;
		}
		if (log.repair) console.error(`[session] ${id}: cut ${log.repair.droppedBytes} torn bytes at offset ${log.repair.offset}`);
		const stored = [...log.entries] as any[];
		const { cwd, messages } = replayEntries(stored);
		const { messages: repaired, added } = repairTranscript(messages, knownResults(stored));
		for (const m of added) log.append({ t: "msg", m });
		const unknownCalls = added.filter((m) => m.details?.interrupted).map((m) => m.toolCallId);
		const { notes, records } = reconcileOps(stored, new Set(unknownCalls), repaired);
		for (const r of records) log.append(r);
		if (added.length) console.error(`[session] ${id}: ${added.length} tool call(s) had no result; marked outcome unknown, not re-run`);
		return { log, cwd, messages: repaired as AgentMessage[], notes, stored };
	}

	/** For records whose whole point is being durable first: the caller must not go on if this throws. */
	private appendStrict(e: unknown): void {
		if (fault("no_persist")) return;
		if (this.persistError) throw this.persistError;
		try {
			this.log.append(e);
		} catch (err: any) {
			this.persistError = err;
			console.error(`[session] ${this.id}: ${err?.message ?? err}`);
			if (err instanceof FencedError) process.exit(75);
			throw err;
		}
	}

	private armThinkWatchdog(): void {
		clearTimeout(this.thinkTimer);
		if (fault("no_think_watchdog")) return;
		// Streaming tool arguments extend the limit, but only so far: a trickle must not keep a request alive forever.
		const left = this.thinkStart + 2 * THINK_MAX_MS - Date.now();
		this.thinkTimer = setTimeout(() => {
			if (this.sawOutput) return;
			this.thinkTimedOut = true;
			this.thinkCtl?.abort();
		}, Math.max(0, Math.min(THINK_MAX_MS, left)));
	}

	private appendEntry(e: unknown): void {
		if (fault("no_persist") || this.persistError) return;
		try {
			this.log.append(e);
		} catch (err: any) {
			// Runs inside an Agent event listener, so throwing here would not reach the caller. Stop the turn: going on
			// would tell the user things worked while the history no longer records them.
			this.persistError = err;
			console.error(`[session] ${this.id}: ${err?.message ?? err}`);
			if (err instanceof FencedError) process.exit(75); // Replaced by a newer process: nothing here may touch state again.
			this.agent?.abort();
		}
	}

	/** What a restart owes: results and steers for the next turn, and answers for old operation ids the model may ask about. */
	restore(notes: any[], stored: any[]): void {
		this.pendingLate.push(...notes);
		this.ops.restoreFinished(stored);
	}

	private onLate(late: LateResult): void {
		if (fault("op_late_lost")) return;
		const target = (fault("op_late_leak") && Session.peers.find((p) => p !== this)) || this;
		// The tool call was already reported finished with the placeholder; this corrects it. Whether the platform shows
		// an update that arrives outside a turn is a separate check against the real client.
		target.send(target.id, {
			sessionUpdate: "tool_call_update",
			toolCallId: late.toolCallId,
			status: late.isError ? "failed" : "completed",
			rawOutput: late.text,
			content: [{ type: "content", content: { type: "text", text: late.text } }],
			_meta: { harness: { lateOp: late.opId } },
		});
		const msg = lateMessage(late);
		if (target.running && !target.cancelled) {
			target.queued.set(`late:${late.opId}`, msg);
			target.agent.steer(msg);
		} else {
			target.pendingLate.push(msg);
		}
	}

	replayHistory(messages: AgentMessage[]): void {
		for (const m of messages as any[]) {
			if (m.role !== "user" && m.role !== "assistant") continue;
			const chunks = (m.content ?? []).filter((b: any) => b.type === "text");
			for (const b of chunks) {
				this.send(this.id, {
					sessionUpdate: m.role === "user" ? "user_message_chunk" : "agent_message_chunk",
					content: { type: "text", text: b.text },
				});
			}
		}
	}

	private async maybeCompact(messages: AgentMessage[], signal: AbortSignal | undefined, force: boolean) {
		const res = await compact(messages, this.cfg, signal, force);
		if (!res) return undefined;
		this.compactions++;
		// The loop's context and the Agent's own transcript are separate arrays; both must move.
		this.agent.state.messages = res.messages;
		this.appendEntry({ t: "compact", messages: res.messages });
		this.lastUsed = res.after;
		this.send(this.id, {
			sessionUpdate: "usage_update",
			used: res.after,
			size: this.cfg.model.contextWindow,
			_meta: { compaction: { summarized: res.summarized, prunedResults: res.prunedResults, before: res.before, after: res.after } },
		});
		console.error(`[compaction] session=${this.id} summarized=${res.summarized} pruned=${res.prunedResults} before=${res.before} after=${res.after}`);
		return res.messages;
	}

	private onEvent(ev: AgentEvent): void {
		switch (ev.type) {
			case "message_update": {
				const e = ev.assistantMessageEvent;
				// Arguments still streaming are progress, not yet output: a large file can take minutes, a stuck call must not hide forever.
				if (e.type === "toolcall_end") this.sawOutput = true;
				else if (e.type.startsWith("toolcall") && !this.sawOutput) this.armThinkWatchdog();
				if (e.type === "text_delta") {
					const { text, thought } = this.splitter.feed(e.delta);
					if (text.trim()) this.sawOutput = true;
					if (thought) this.send(this.id, { sessionUpdate: "agent_thought_chunk", content: { type: "text", text: thought } });
					if (text) this.send(this.id, { sessionUpdate: "agent_message_chunk", content: { type: "text", text } });
				} else if (e.type === "thinking_delta" && !fault("no_thought")) {
					this.send(this.id, { sessionUpdate: "agent_thought_chunk", content: { type: "text", text: e.delta } });
				}
				break;
			}
			case "message_start": {
				const m: any = ev.message;
				if (m.role === "assistant") this.splitter = new InlineThinkSplitter();
				if (m.role === "user" && m.zgSteer) { // Also a steer recovered after a restart or an error: it is delivered just the same.
					// A queued steer just entered the transcript: the next model call sees it.
					this.pendingSteers = this.pendingSteers.filter((s) => s !== m.zgSteer);
					this.send(this.id, {
						sessionUpdate: "agent_thought_chunk",
						content: { type: "text", text: `${STEER_DELIVERY}${JSON.stringify({ outcome: "delivered", text: m.zgSteer, source: "steer" })}` },
					});
				}
				break;
			}
			case "message_end": {
				const m: any = ev.message;
				if (m.role === "assistant") clearTimeout(this.thinkTimer);
				stripInlineThink(m);
				this.appendEntry({ t: "msg", m });
				if (m.zgLateOp) {
					this.queued.delete(`late:${m.zgLateOp}`);
					this.deliveredLate.push(m.zgLateOp);
					if (!fault("op_no_told")) this.appendEntry({ t: "op_told", opId: m.zgLateOp });
				}
				// Durable is not the same as known to the model. Once it has produced an answer from a request carrying the result,
				// it knows, and calling the same thing again is its decision (the user may well have asked for that). An empty answer
				// is dropped and retried below, so it does not count. What this cannot see: a compaction between the result and that
				// request could summarize the result away. Stage 1 needs an idempotency key the server honours for that.
				if (m.role === "assistant" && m.stopReason !== "error" && m.stopReason !== "aborted" && !producedNothing(m)) {
					for (const opId of this.deliveredLate.splice(0)) this.ops.markTold(opId);
				}
				if (m.zgSteerId) {
					this.queued.delete(`steer:${m.zgSteerId}`);
					this.pendingSteerIds.delete(m.zgSteerId);
					this.appendEntry({ t: "steer_done", id: m.zgSteerId });
				}
				if (m.role === "assistant" && m.usage) {
					const used = m.usage.input + m.usage.cacheRead + m.usage.cacheWrite + m.usage.output + (fault("used_cumulative") ? this.lastUsed : 0);
					if (used > 0) {
						this.lastUsed = used;
						this.send(this.id, { sessionUpdate: "usage_update", used, size: this.cfg.model.contextWindow });
					}
				}
				break;
			}
			case "tool_execution_start":
				this.turnHadEffects = true;
				this.send(this.id, {
					sessionUpdate: "tool_call",
					toolCallId: ev.toolCallId,
					title: titleFor(ev.toolName, ev.args),
					kind: kindFor(ev.toolName),
					status: "in_progress",
					rawInput: ev.args,
				});
				break;
			case "tool_execution_update":
				this.send(this.id, {
					sessionUpdate: "tool_call_update",
					toolCallId: ev.toolCallId,
					status: "in_progress",
					content: [{ type: "content", content: { type: "text", text: resultText(ev.partialResult).slice(-4000) } }],
				});
				break;
			case "tool_execution_end": {
				const out = resultText(ev.result);
				this.send(this.id, {
					sessionUpdate: "tool_call_update",
					toolCallId: ev.toolCallId,
					status: ev.isError ? "failed" : "completed",
					rawOutput: out,
					content: [{ type: "content", content: { type: "text", text: out } }],
				});
				break;
			}
		}
	}

	steer(text: string, expectedTurn?: string): void {
		if (!this.running && fault("idle_steer_starts_turn")) {
			void this.prompt(text);
			return;
		}
		// A cancelled turn is winding down: accepted now, this steer would never reach a model call.
		if (!this.running || this.cancelled) throw new RpcError(-32050, "steer_idle_rejected: no active turn");
		if (expectedTurn && this.turnToken && expectedTurn !== this.turnToken) {
			throw new RpcError(-32050, "steer_turn_mismatch");
		}
		// Durable before it is accepted: the platform shows the steer as taken, so a crash must not lose it.
		const id = randomUUID();
		if (!fault("steer_not_durable")) {
			try {
				this.appendStrict({ t: "steer_recv", id, text });
			} catch (err: any) {
				throw new RpcError(-32000, `steer not accepted: ${err?.message ?? err}`);
			}
		}
		this.turnHadEffects = true;
		this.pendingSteers.push(text);
		this.pendingSteerIds.set(id, text);
		const msg = { role: "user", content: [{ type: "text", text }], timestamp: Date.now(), zgSteer: text, zgSteerId: id };
		this.queued.set(`steer:${id}`, msg);
		this.agent.steer(msg as any);
		this.table.yieldForeground();
		this.ops.yieldAll();
	}

	cancel(): void {
		if (!fault("cancel_keeps_queued")) this.ops.dropAllQueued();
		if (!this.running) return;
		this.cancelled = true;
		this.turnAbort.abort();
		this.agent.clearAllQueues();
		// The queue was just emptied. Without this a restart would bring back what the user cancelled.
		for (const id of this.pendingSteerIds.keys()) {
			this.appendEntry({ t: "steer_done", id, cancelled: true });
			this.queued.delete(`steer:${id}`);
		}
		this.pendingSteerIds.clear();
		this.requeueAccepted();
		this.agent.abort();
	}

	/** What was accepted is owed to the model whatever happens to the turn. The Agent's queue is empty now, so take it back. */
	private requeueAccepted(): void {
		if (fault("cancel_drops_late")) {
			this.queued.clear();
			return;
		}
		const items = [...this.queued.values()];
		if (fault("requeue_unordered")) items.sort((x, y) => Number(Boolean(y.zgLateOp)) - Number(Boolean(x.zgLateOp)));
		this.pendingLate.push(...items);
		this.queued.clear();
		this.pendingSteerIds.clear();
	}

	async prompt(text: string, turnToken?: string): Promise<Record<string, unknown>> {
		if (this.running) throw new RpcError(-32000, "session busy: a turn is already running");
		// With the log broken nothing would record what the next turn's tools do.
		if (this.persistError) throw new RpcError(-32000, `session log write failed: ${this.persistError.message}`);
		this.running = true;
		this.cancelled = false;
		this.turnAbort = new AbortController();
		this.guard.reset();
		this.turnToken = turnToken;
		this.turnHadEffects = false;
		const savedLevel = this.agent.state.thinkingLevel;
		let overflowRetried = false;
		let thinkRetries = 0;
		let transientRetries = 0;
		try {
			if (fault("cross_session_leak")) {
				const prior = Session.lastPrompt;
				Session.lastPrompt = text;
				await this.agent.prompt(prior ? `${text}\n(earlier: ${prior})` : text);
			} else if (this.pendingLate.length) {
				const late = this.pendingLate.splice(0);
				await this.agent.prompt([...late, { role: "user", content: [{ type: "text", text }], timestamp: Date.now() }]);
			} else {
				await this.agent.prompt(text);
			}
			for (;;) {
				if (this.cancelled) break;
				const last: any = this.agent.state.messages.at(-1);
				if (last?.role === "assistant" && last.stopReason === "error" && isContextOverflow(last, this.cfg.model.contextWindow)) {
					this.agent.state.messages = this.agent.state.messages.slice(0, -1);
					this.appendEntry({ t: "drop_last", reason: "overflow" });
					const res = overflowRetried ? undefined : await this.maybeCompact(this.agent.state.messages, this.turnAbort.signal, true).catch(() => undefined);
					overflowRetried = true;
					if (this.cancelled) break;
					if (res) {
						await this.agent.continue();
						continue;
					}
					// Tools already ran. Taking the prompt back would also erase what they did, and a user retrying would run them again.
					if (this.turnHadEffects && !fault("overflow_erases_effects")) {
						throw new RpcError(-32000, `context is full (${this.cfg.model.contextWindow} tokens) and could not be compacted; what this turn already did is kept`);
					}
					// Compaction cannot help: the prompt alone exceeds the window. Refuse it and drop it,
					// otherwise it stays in the transcript and poisons every later turn.
					const kept = this.agent.state.messages;
					const lastUser = kept.findLastIndex((m: any) => m.role === "user");
					this.agent.state.messages = lastUser >= 0 ? kept.slice(0, lastUser) : kept;
					this.appendEntry({ t: "compact", messages: this.agent.state.messages });
					throw new RpcError(-32000, `prompt does not fit the context window (${this.cfg.model.contextWindow} tokens)`);
				}
				if (last?.role === "assistant" && last.stopReason === "error" && !fault("no_stream_retry") && transientRetries < TRANSIENT_RETRIES && TRANSIENT_ERROR.test(String(last.errorMessage ?? ""))) {
					transientRetries++;
					console.error(`[turn] session=${this.id} transient model error, retry ${transientRetries}/${TRANSIENT_RETRIES}: ${last.errorMessage}`);
					this.agent.state.messages = this.agent.state.messages.slice(0, -1);
					this.appendEntry({ t: "drop_last", reason: "transient" });
					await new Promise((r) => setTimeout(r, TRANSIENT_BACKOFF_MS * transientRetries));
					if (this.cancelled) break;
					await this.agent.continue();
					continue;
				}
				const stoppedEmpty = last?.stopReason === "stop" || last?.stopReason === "length";
				if (producedNothing(last) && (stoppedEmpty || (last.stopReason === "aborted" && this.thinkTimedOut)) && !fault("no_think_guard")) {
					// Ending the turn here looks like success and tells the user nothing. Drop the empty answer and try
					// once without thinking; if that is empty too, say so instead of ending quietly.
					this.agent.state.messages = this.agent.state.messages.slice(0, -1);
					this.appendEntry({ t: "drop_last", reason: last.stopReason });
					if (thinkRetries >= THINK_RETRIES) {
						throw new RpcError(-32000, "模型没有给出回复（输出全部用在思考上，或上下文已满）。可以换个说法、把任务拆小后再试。");
					}
					thinkRetries++;
					// A nearly full context also comes back empty: there is no room left to answer. Thinking has nothing to
					// do with it then, and retrying at the same size is empty again, so make room first.
					const crowded = estimateContext(this.agent.state.messages) > this.cfg.model.contextWindow * CROWDED_RATIO;
					if (crowded) await this.maybeCompact(this.agent.state.messages, this.turnAbort.signal, true).catch(() => undefined);
					if (this.cancelled) break;
					console.error(`[turn] session=${this.id} empty answer (${last.stopReason}, watchdog=${this.thinkTimedOut}, crowded=${crowded}); retrying with thinking off`);
					// "Off" only works where the model profile knows how to turn thinking off, so say it in words as well.
					const nudge = { role: "user", content: [{ type: "text", text: "[上一次回复全部花在思考上，没有任何产出。不要再长时间思考，现在直接给出下一步行动或结论。]" }], timestamp: Date.now() };
					this.agent.state.messages = [...this.agent.state.messages, nudge as any];
					this.appendEntry({ t: "msg", m: nudge });
					// Stays off until the turn ends: a later stream retry must not turn thinking back on and use up the budget again.
					this.agent.state.thinkingLevel = "off";
					await this.agent.continue();
					if (fault("think_off_one_call")) this.agent.state.thinkingLevel = savedLevel;
					continue;
				}
				// Nothing more will be tried. The broken answer must not stay in the history the next request is built from.
				if (last?.role === "assistant" && last.stopReason === "error" && !fault("error_answer_stays")) {
					this.agent.state.messages = this.agent.state.messages.slice(0, -1);
					this.appendEntry({ t: "drop_last", reason: "error" });
					throw new RpcError(-32000, last.errorMessage ?? "model error");
				}
				// A steer can land after the loop's last poll, while agent_end listeners settle.
				if (this.agent.hasQueuedMessages() && last?.stopReason !== "error") {
					await this.agent.continue();
					continue;
				}
				break;
			}
			const logError = this.persistError as Error | undefined; // TypeScript narrowed this at the top; listeners set it since.
			if (logError) throw new RpcError(-32000, `session log write failed: ${logError.message}`);
			if (this.guard.tripped) throw new RpcError(-32000, `tool loop stopped: ${this.guard.tripped}`);
			return this.finishResult();
		} finally {
			clearTimeout(this.thinkTimer);
			this.running = false;
			this.pendingSteers = [];
			this.agent.state.thinkingLevel = savedLevel;
			// An error can end the turn with things still queued. Whatever was accepted goes first next turn, in the same order.
			if (this.queued.size) {
				this.agent.clearAllQueues();
				this.requeueAccepted();
			}
		}
	}

	private finishResult(): Record<string, unknown> {
		const last: any = [...this.agent.state.messages].reverse().find((m: any) => m.role === "assistant");
		const usage = {
			inputTokens: last?.usage?.input ?? 0,
			outputTokens: last?.usage?.output ?? 0,
			used: this.lastUsed,
			size: this.cfg.model.contextWindow,
			contextUsedTokens: this.lastUsed,
			contextTokens: this.cfg.model.contextWindow,
		};
		const meta = { harness: {}, kit: { estimate: estimateContext(this.agent.state.messages), compactions: this.compactions, opsInFlight: this.ops.inFlight() } };
		if (this.cancelled || last?.stopReason === "aborted") return { stopReason: "cancelled", usage, _meta: meta };
		if (last?.stopReason === "error") {
			throw new RpcError(-32000, last.errorMessage ?? "model error");
		}
		return { stopReason: last?.stopReason === "length" ? "max_tokens" : "end_turn", usage, _meta: meta };
	}

	dispose(): void {
		this.table.killAll();
		this.log.close();
	}
}
