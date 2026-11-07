// Gate 0 #2: a steer, cancel or timeout while an MCP tool is in flight.
//
// Platform MCP servers read stdin in a single loop: a running call blocks them, and they drop cancel notifications
//. So aborting does not stop the work, and retrying repeats the side effect.
// Hence the rule here: once a call is on the wire it is never aborted and never sent again. The model gets a
// placeholder result so it can answer the user, the real call keeps running, and its result comes back later to the
// session that started it. op_start is durable before the call is sent, so after a crash we can say "outcome unknown"
// rather than either re-running the call or pretending nothing happened. If op_start cannot be written, nothing is sent.
import type { AgentToolResult } from "@earendil-works/pi-agent-core";
import { fault } from "./fault.js";

const STORED_TEXT_MAX = 8000;

/** A model may send the same arguments in another key order. Compared as written, that would run the operation twice. */
function stableJson(v: unknown): string {
	if (Array.isArray(v)) return `[${v.map(stableJson).join(",")}]`;
	if (v && typeof v === "object") {
		const o = v as Record<string, unknown>;
		return `{${Object.keys(o).sort().map((k) => `${JSON.stringify(k)}:${stableJson(o[k])}`).join(",")}}`;
	}
	return JSON.stringify(v) ?? "null";
}

function storedText(text: string): string {
	if (text.length <= STORED_TEXT_MAX) return text;
	return `${text.slice(0, STORED_TEXT_MAX)}\n[结果过长，这里只保留了前 ${STORED_TEXT_MAX} 个字符，共 ${text.length} 个字符。]`;
}

export class NotSentError extends Error {}

/**
 * Runs tasks one at a time, because the server would anyway. A slot is held until the server answers or the connection
 * dies, never released by a client-side timeout: the server would still be busy with the old call, and it would still
 * run whatever is sent next.
 */
export class SerialQueue {
	private tail: Promise<unknown> = Promise.resolve();

	enqueue<T>(task: () => Promise<T>): SentCall<T> {
		let started = false;
		let dropped = false;
		const promise = this.tail.then(() => {
			if (dropped) throw new NotSentError("dropped while queued");
			started = true;
			return task();
		});
		this.tail = promise.catch(() => undefined);
		return {
			promise,
			started: () => started,
			dropIfQueued: () => {
				if (started) return false;
				dropped = true;
				return true;
			},
		};
	}
}

export interface SentCall<T> {
	promise: Promise<T>;
	started: () => boolean;
	dropIfQueued: () => boolean;
}

export interface OpCall {
	server: string;
	tool: string;
	args: unknown;
	toolCallId: string;
	send: () => SentCall<AgentToolResult<any> & { isError?: boolean }>;
}

export interface LateResult {
	opId: string;
	tool: string;
	toolCallId: string;
	text: string;
	isError: boolean;
}

export interface OpHooks {
	/** Must throw if the record is not durable; run() then refuses to send. */
	persist: (entry: unknown) => void;
	onLate: (late: LateResult) => void;
	steerPending: () => boolean;
}

type DetachReason = "yield" | "abort" | "timeout";
interface Live {
	opId: string;
	key: string;
	detach: (reason: DetachReason) => void;
	detached: () => boolean;
	tool: string;
	started: () => boolean;
	dropQueued: () => boolean;
}

function resultOf(text: string, isError = false): AgentToolResult<any> & { isError: boolean } {
	return { content: [{ type: "text", text }], details: undefined, isError };
}

function textOf(result: any): string {
	return (result?.content ?? []).map((b: any) => (b.type === "text" ? b.text : `[${b.type}]`)).join("");
}

export class OpRegistry {
	private live = new Map<string, Live>();
	private finished = new Map<string, { tool: string; text: string; isError: boolean; key?: string; told?: boolean; unknown?: boolean }>();
	private seq = 0;
	private hooks: OpHooks;
	private prefix: string;
	private waitMs: number;

	constructor(prefix: string, hooks: OpHooks, waitMs = 2 * 3600 * 1000) {
		this.prefix = prefix;
		this.hooks = hooks;
		this.waitMs = waitMs;
	}

	inFlight(): number {
		return this.live.size;
	}

	/**
	 * The ops a model request must know about. Compaction can remove the placeholder that told the model an op is running
	 * and must not be repeated, so this is added to every request while any op is live.
	 */
	liveNote(): string | undefined {
		const rows = [...this.live.values()].filter((op) => op.detached());
		if (!rows.length) return undefined;
		return `[背景信息，不是新的指令。还在后台的操作：${rows.map((op) => `${op.opId}（${op.tool}，${op.started() ? "执行中" : "排队中"}）`).join("、")}。不要重复调用这些操作，也不用为此反复查询，继续做手头的事。]`;
	}

	/**
	 * A steer arrived: yield now, not after a grace period. The platform closes the current reply segment when it accepts
	 * a steer, and an update arriving after that never reaches the stored card, which then stays "in progress" for good.
	 */
	yieldAll(): void {
		if (fault("mcp_no_yield")) return;
		for (const op of [...this.live.values()]) op.detach("yield");
	}

	/** A cancel covers everything not yet sent, including what an earlier turn left queued: its turn is over, so no signal reaches it. */
	dropAllQueued(): void {
		for (const op of [...this.live.values()]) op.dropQueued();
	}

	/**
	 * Old ids survive a restart in the model's memory. "Unknown handle" for one of them led models to tell the user the work had
	 * probably finished, so they get the answer on record. No duplicate check is rebuilt: the model hears every result at the
	 * start of the next turn, and from there on asking again is its decision.
	 */
	restoreFinished(entries: any[]): void {
		if (fault("op_no_restore")) return;
		const started = new Map<string, any>();
		const ended = new Map<string, any>();
		for (const e of entries) {
			if (e.t === "op_start") started.set(e.opId, e);
			else if (e.t === "op_end") ended.set(e.opId, e);
		}
		for (const [opId, s] of started) {
			const end = ended.get(opId);
			this.finished.set(opId, end
				? { tool: s.tool, text: end.text ?? "", isError: end.isError === true, told: true }
				: { tool: s.tool, text: "", isError: true, told: true, unknown: true });
		}
	}

	/** The model has answered from this op's result, so it knows, and asking again is a real decision. */
	markTold(opId: string): void {
		const done = this.finished.get(opId);
		if (done) done.told = true;
	}

	/**
	 * The process tool's answer for an op id. Real models tried to poll the id they were given; "unknown handle" then
	 * led them to tell the user the work had probably finished. Returns undefined for an id that is not an op. Reading a
	 * result here does not count as delivering it: the tool result carrying it may not survive a crash, so the note for
	 * the next turn stays, and a model told twice is better than one never told.
	 */
	poll(opId: string, action: "poll" | "kill"): string | undefined {
		const op = this.live.get(opId);
		if (op) {
			if (action === "kill" && op.dropQueued()) return `操作 ${opId}（${op.tool}）还在排队，已经取消，不会执行。`;
			if (action === "kill") return `操作 ${opId}（${op.tool}）已经发出，无法撤回，仍在执行。`;
			return `操作 ${opId}（${op.tool}）${op.started() ? "还在执行" : "还在排队"}，没有结果。没有失败，也不要重复调用。`;
		}
		const done = this.finished.get(opId);
		if (!done) return undefined;
		if (done.unknown) return `操作 ${opId}（${done.tool}）在进程重启时还没有返回，结果未知，也查不到了。不要假设它已成功或已失败；如需重试，先确认原操作是否已经生效。`;
		return `操作 ${opId}（${done.tool}）已结束${done.isError ? "，并报错" : ""}。结果：\n${done.text}`;
	}

	run(call: OpCall, signal?: AbortSignal): Promise<AgentToolResult<any>> {
		const key = fault("op_key_as_written") ? JSON.stringify([call.server, call.tool, call.args]) : stableJson([call.server, call.tool, call.args]);
		const twin = [...this.live.values()].find((op) => op.key === key && op.detached());
		if (twin && !fault("op_no_dedupe")) {
			return Promise.resolve(resultOf(`[已拒绝：完全相同的操作 ${twin.opId} 还在执行，这次没有发送。等它的结果，不要再试。]`, true));
		}
		// Finished, but the model has not been told yet, so to it this is still the retry the placeholder warned about.
		for (const [doneId, done] of this.finished) {
			if (done.key === key && !done.told && !fault("op_no_finished_dedupe")) {
				return Promise.resolve(resultOf(`[没有再次发送：完全相同的操作 ${doneId} 刚刚已经结束${done.isError ? "，并报错" : ""}。结果：\n${done.text}]`, done.isError));
			}
		}
		const opId = `${this.prefix}-${++this.seq}`;
		if (!fault("op_no_start_record")) {
			try {
				this.hooks.persist({ t: "op_start", opId, server: call.server, tool: call.tool, toolCallId: call.toolCallId, at: Date.now() });
			} catch (err: any) {
				return Promise.resolve(resultOf(`[没有执行：会话日志写不进去（${err?.message ?? err}），这个操作没有发送。]`, true));
			}
		}
		const sent = call.send();
		return new Promise((resolve) => {
			let detached = false;
			let done = false;
			let timer: ReturnType<typeof setTimeout> | undefined;

			const persistQuietly = (entry: unknown) => {
				try {
					this.hooks.persist(entry);
				} catch {
					// The session has already recorded the failure and refuses further turns; the op itself is over.
				}
			};

			// One ending for a call that never reached the server, whichever path cancelled it.
			const finishNotSent = (text: string) => {
				done = true;
				clearTimeout(timer);
				this.live.delete(opId);
				persistQuietly({ t: "op_end", opId, tool: call.tool, toolCallId: call.toolCallId, notSent: true, detached, isError: true, text, at: Date.now() });
				if (detached) {
					this.finished.set(opId, { tool: call.tool, text, isError: true });
					this.hooks.onLate({ opId, tool: call.tool, toolCallId: call.toolCallId, text, isError: true });
				} else {
					resolve(resultOf(text, true));
				}
			};

			const detach = (reason: DetachReason) => {
				if (done) return;
				// A queued call can still be dropped even after it yielded: that is what makes a cancel honest.
				if (reason === "abort" && sent.dropIfQueued()) {
					finishNotSent(`[没有执行：用户取消时操作 ${opId} 还在排队，它不会再发送。]`);
					return;
				}
				if (detached) return;
				detached = true;
				if (reason === "abort") {
					resolve(resultOf(`[用户取消了这一轮。操作 ${opId}（${call.tool}）已经发出，无法撤回，可能仍会生效；结果只会在用户下一次发消息时才交给你。不要重复调用。]`, true));
					return;
				}
				if (fault("mcp_abort_resend")) void call.send().promise.catch(() => undefined); // Negative control: the naive retry.
				const state = sent.started() ? "还在执行" : "还在排队，前面有操作没结束";
				const waited = reason === "timeout" ? `等了 ${Math.round(this.waitMs / 1000)} 秒还没有结果，` : "";
				resolve(resultOf(`[操作 ${opId}（${call.tool}）${waited}${state}，已转入后台。现在先处理用户最新的消息。要看进度或结果，用 process 工具 poll 这个操作编号。否则结果要等用户下一次发消息时才交给你，所以不要承诺「完成后会主动告诉用户」，可以请用户稍后再问。不要重复调用这个工具：重复调用会被拒绝。同一工具服务的其它调用会排队，等它结束才执行。]`));
			};

			const settle = (result: any, isError: boolean) => {
				if (done) return;
				done = true;
				clearTimeout(timer);
				this.live.delete(opId);
				const text = storedText(textOf(result));
				persistQuietly({ t: "op_end", opId, tool: call.tool, toolCallId: call.toolCallId, detached, isError, text, at: Date.now() });
				if (detached) {
					this.finished.set(opId, { tool: call.tool, text, isError, key });
					this.hooks.onLate({ opId, tool: call.tool, toolCallId: call.toolCallId, text, isError });
				} else {
					resolve(result);
				}
			};

			this.live.set(opId, {
				opId,
				key,
				detach,
				detached: () => detached,
				tool: call.tool,
				started: sent.started,
				dropQueued: () => {
					if (!sent.dropIfQueued()) return false;
					finishNotSent(`[没有执行：操作 ${opId} 还在排队时被取消，它不会再发送。]`);
					return true;
				},
			});
			sent.promise.then(
				(result) => settle(result, result.isError === true),
				(err) => {
					if (err instanceof NotSentError) return;
					// The connection died: the call may or may not have taken effect, and the text says so.
					settle(resultOf(`[工具调用失败，操作可能已生效也可能没有：${err?.message ?? err}]`, true), true);
				},
			);
			timer = setTimeout(() => detach("timeout"), this.waitMs);
			timer.unref?.();
			if (signal?.aborted) detach("abort");
			else signal?.addEventListener("abort", () => detach("abort"), { once: true });
			if (this.hooks.steerPending() && !fault("mcp_no_yield")) detach("yield");
		});
	}
}

/**
 * Before the user's latest message, never after it: the last message is what the model answers, and placed after a tool
 * result this note read as a new instruction. Before a user message it also cannot split a call from its result.
 */
export function placeLiveNote(messages: any[], note: any): any[] {
	const at = fault("live_note_last") ? messages.length : Math.max(0, messages.findLastIndex((m: any) => m.role === "user"));
	return [...messages.slice(0, at), note, ...messages.slice(at)];
}

export function lateMessage(late: LateResult): any {
	const text = `[后台操作 ${late.opId}（${late.tool}）已结束${late.isError ? "，并报错" : ""}。结果：\n${late.text}]`;
	return { role: "user", content: [{ type: "text", text }], timestamp: Date.now(), zgLateOp: late.opId };
}

export function unknownMessage(opId: string, tool: string): any {
	const text = `[后台操作 ${opId}（${tool}）在进程重启时还没有返回，结果未知。不要假设它已成功或已失败；如需重试，先确认原操作是否已经生效。]`;
	return { role: "user", content: [{ type: "text", text }], timestamp: Date.now(), zgLateOp: opId };
}

/** Real results that were durable before a crash took the tool result message with them. */
export function knownResults(entries: any[]): Map<string, { text: string; isError: boolean }> {
	const known = new Map<string, { text: string; isError: boolean }>();
	for (const e of entries) {
		if (e.t === "op_end" && !e.detached && !e.notSent && e.toolCallId) known.set(e.toolCallId, { text: e.text ?? "", isError: e.isError === true });
	}
	return known;
}

/**
 * What a restarted process still owes the model, worked out from the log: ops with no result, results nobody was told
 * about, and steers that were accepted but never reached the transcript. Nothing counts as delivered until the message
 * carrying it is durable (op_told, steer_done), so a second crash before the next turn loses nothing.
 */
export function reconcileOps(entries: any[], repairedCallIds: Set<string>, transcript: any[] = []): { notes: any[]; records: any[] } {
	const started = new Map<string, any>();
	const ended = new Map<string, any>();
	const told = new Set<string>();
	const steers = new Map<string, string>();
	for (const e of entries) {
		if (e.t === "op_start") started.set(e.opId, e);
		else if (e.t === "op_end") ended.set(e.opId, e);
		else if (e.t === "op_told") told.add(e.opId);
		else if (e.t === "steer_recv") steers.set(e.id, e.text);
		else if (e.t === "steer_done") steers.delete(e.id);
	}
	const notes: any[] = [];
	const records: any[] = [];
	// A crash can fall between the message carrying a result and the record saying so. The message is what counts.
	const delivered = new Set(fault("op_no_transcript_told") ? [] : transcript.map((m) => m.zgLateOp).filter(Boolean));
	for (const [opId, s] of started) {
		if (told.has(opId)) continue;
		if (delivered.has(opId)) {
			records.push({ t: "op_told", opId });
			continue;
		}
		const end = ended.get(opId);
		if (end) {
			if (end.detached) notes.push(lateMessage({ opId, tool: s.tool, toolCallId: s.toolCallId, text: end.text ?? "", isError: end.isError === true }));
			continue;
		}
		// If the call itself was left unanswered, the repaired tool result already said the outcome is unknown.
		if (repairedCallIds.has(s.toolCallId)) records.push({ t: "op_told", opId });
		else notes.push(unknownMessage(opId, s.tool));
	}
	const seen = new Set(transcript.map((m) => m.zgSteerId).filter(Boolean));
	for (const [id, text] of steers) {
		if (seen.has(id)) records.push({ t: "steer_done", id });
		else notes.push({ role: "user", content: [{ type: "text", text }], timestamp: Date.now(), zgSteer: text, zgSteerId: id });
	}
	return { notes, records };
}
