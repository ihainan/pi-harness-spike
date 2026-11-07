// terminal / process / read_file / write_file. terminal runs each command in its own process
// group (setsid via detached), kills TERM→KILL, keeps head+tail of output, and can "yield":
// a steer moves the foreground process to the background and returns a handle immediately.
import { type ChildProcess, spawn } from "node:child_process";
import * as fs from "node:fs/promises";
import * as path from "node:path";
import type { AgentTool, AgentToolResult } from "@earendil-works/pi-agent-core";
import { fault } from "./fault.js";

const HEAD = 8 * 1024;
const TAIL = 8 * 1024;
const KILL_GRACE_MS = 2000;
const EXIT_DRAIN_MS = 200; // a background grandchild may hold the pipe open after exit
const READ_MAX = 64 * 1024;

class OutputBuffer {
	head = "";
	tail = "";
	dropped = 0;
	push(s: string): void {
		if (this.head.length < HEAD) {
			const take = s.slice(0, HEAD - this.head.length);
			this.head += take;
			s = s.slice(take.length);
		}
		if (!s) return;
		this.tail += s;
		if (this.tail.length > TAIL) {
			this.dropped += this.tail.length - TAIL;
			this.tail = this.tail.slice(-TAIL);
		}
	}
	text(): string {
		if (this.dropped) return `${this.head}\n...[${this.dropped} chars truncated]...\n${this.tail}`;
		return this.head + this.tail;
	}
}

interface ProcRec {
	handle: string;
	child: ChildProcess;
	command: string;
	out: OutputBuffer;
	startedAt: number;
	exitCode?: number | null;
	signal?: NodeJS.Signals | null;
	exited: Promise<void>;
}

function killGroup(pid: number | undefined, sig: NodeJS.Signals): void {
	if (!pid) return;
	try {
		process.kill(fault("no_group_kill") ? pid : -pid, sig);
	} catch {
		/* group gone */
	}
}

async function terminate(rec: ProcRec): Promise<void> {
	killGroup(rec.child.pid, "SIGTERM");
	const gone = await Promise.race([
		rec.exited.then(() => true),
		new Promise<boolean>((r) => setTimeout(() => r(false), KILL_GRACE_MS)),
	]);
	// Always KILL the group: the leader may be gone while a TERM-ignoring member survives.
	killGroup(rec.child.pid, "SIGKILL");
	if (!gone) await Promise.race([rec.exited, new Promise((r) => setTimeout(r, 1000))]);
}

function text(t: string, isError = false): AgentToolResult<undefined> {
	return { content: [{ type: "text", text: t }], details: undefined, isError };
}

export class ProcessTable {
	procs = new Map<string, ProcRec>();
	private seq = 0;
	private yielders = new Set<() => void>();
	/** Set by the session: a steer is queued and not yet delivered to the model. */
	steerPending: () => boolean = () => false;
	pollOp: (handle: string, action: "poll" | "kill") => string | undefined = () => undefined;

	nextHandle(): string {
		return `bg-${++this.seq}`;
	}
	yieldForeground(): void {
		if (fault("no_yield")) return;
		for (const y of [...this.yielders]) y();
	}
	addYielder(y: () => void): () => void {
		this.yielders.add(y);
		return () => this.yielders.delete(y);
	}
	killAll(): void {
		for (const rec of this.procs.values()) {
			if (rec.exitCode === undefined) {
				killGroup(rec.child.pid, "SIGKILL");
			}
		}
	}
}

export function makeBuiltinTools(cwd: string, table: ProcessTable): AgentTool<any>[] {
	const resolve = (p: string) => (path.isAbsolute(p) ? p : path.join(cwd, p));

	const terminal: AgentTool<any> = {
		name: "terminal",
		label: "terminal",
		description:
			"Run a shell command in the foreground and wait for it. If the user interjects while it runs, the command " +
			"moves to the background and you get a handle; use the process tool to poll or kill it.",
		parameters: {
			type: "object",
			properties: {
				command: { type: "string", description: "Shell command" },
				timeout_s: { type: "number", description: "Foreground timeout in seconds (default 600)" },
			},
			required: ["command"],
		},
		executionMode: "parallel",
		execute: (_id, params, signal, onUpdate) =>
			new Promise((resolvePromise, rejectPromise) => {
				const child = spawn("bash", ["-c", params.command], {
					cwd,
					detached: true,
					stdio: ["ignore", "pipe", "pipe"],
				});
				const out = new OutputBuffer();
				let markExited = () => {};
				const rec: ProcRec = {
					handle: table.nextHandle(),
					child,
					command: params.command,
					out,
					startedAt: Date.now(),
					exited: new Promise<void>((r) => {
						markExited = r;
					}),
				};
				table.procs.set(rec.handle, rec);
				let settled = false;
				let lastUpdate = 0;
				const onChunk = (b: Buffer) => {
					out.push(b.toString("utf8"));
					if (settled || !onUpdate) return;
					const now = Date.now();
					if (now - lastUpdate > 1000) {
						lastUpdate = now;
						onUpdate(text(out.text()));
					}
				};
				child.stdout!.on("data", onChunk);
				child.stderr!.on("data", onChunk);

				// The model picks the timeout; T8L saw it ask for 999999s on a `while true`. Hard cap.
				const timeoutMs = Math.min(params.timeout_s ?? 600, Number(process.env.PI_MAX_TIMEOUT_S ?? 1800)) * 1000;
				const cleanup = () => {
					settled = true;
					clearTimeout(timer);
					clearTimeout(steerTimer);
					signal?.removeEventListener("abort", onAbort);
					removeYielder();
				};
				const onAbort = () => {
					if (settled) return;
					cleanup();
					terminate(rec).finally(() => {
						table.procs.delete(rec.handle);
						rejectPromise(new Error(`command cancelled; process group ${child.pid} killed`));
					});
				};
				const timer = setTimeout(() => {
					if (settled) return;
					cleanup();
					terminate(rec).finally(() => {
						table.procs.delete(rec.handle);
						resolvePromise(text(`command timed out after ${timeoutMs / 1000}s and was killed\n${out.text()}`, true));
					});
				}, timeoutMs);
				const removeYielder = table.addYielder(() => {
					if (settled) return;
					cleanup();
					const secs = ((Date.now() - rec.startedAt) / 1000).toFixed(1);
					resolvePromise(
						text(
							`The user interjected. The command is STILL RUNNING in the background (handle=${rec.handle}, pid=${child.pid}, ` +
								`running ${secs}s). Use the process tool to poll or kill it.\nOutput so far:\n${out.text()}`,
						),
					);
				});
				// Steer arrived while the model was still streaming this very tool call: give fast
				// commands a second to finish, then yield so the steer is not stuck behind us.
				const steerTimer = table.steerPending()
					? setTimeout(() => table.yieldForeground(), 1000)
					: (undefined as unknown as NodeJS.Timeout);
				if (signal?.aborted) onAbort();
				else signal?.addEventListener("abort", onAbort, { once: true });

				child.on("error", (err) => {
					if (settled) return;
					cleanup();
					rejectPromise(err);
				});
				child.on("exit", (code, sig) => {
					rec.exitCode = code;
					rec.signal = sig;
					markExited();
					setTimeout(() => {
						if (settled) return;
						cleanup();
						table.procs.delete(rec.handle);
						const failed = code !== 0;
						resolvePromise(
							text(`${out.text()}\n[exit ${code ?? `signal ${sig}`}]`.trimStart(), failed),
						);
					}, EXIT_DRAIN_MS);
				});
			}),
	};

	const processTool: AgentTool<any> = {
		name: "process",
		label: "process",
		description: "Poll or kill a background command by its handle, or check a background operation by its operation id.",
		parameters: {
			type: "object",
			properties: { action: { type: "string", enum: ["poll", "kill"] }, handle: { type: "string" } },
			required: ["action", "handle"],
		},
		execute: async (_id, params) => {
			const rec = table.procs.get(params.handle);
			if (!rec) {
				const op = table.pollOp(params.handle, params.action);
				return op === undefined ? text(`unknown handle ${params.handle}`, true) : text(op);
			}
			if (params.action === "kill" && rec.exitCode === undefined) await terminate(rec);
			const running = rec.exitCode === undefined;
			return text(`${running ? "running" : `exited ${rec.exitCode ?? rec.signal}`}\n${rec.out.text()}`);
		},
	};

	const readFile: AgentTool<any> = {
		name: "read_file",
		label: "read_file",
		description: "Read a UTF-8 text file.",
		parameters: { type: "object", properties: { path: { type: "string" } }, required: ["path"] },
		execute: async (_id, params) => {
			const data = await fs.readFile(resolve(params.path), "utf8");
			if (data.length <= READ_MAX) return text(data);
			return text(`${data.slice(0, READ_MAX)}\n...[truncated, ${data.length} chars total]`);
		},
	};

	const writeFile: AgentTool<any> = {
		name: "write_file",
		label: "write_file",
		description: "Write a UTF-8 text file, creating parent directories.",
		parameters: {
			type: "object",
			properties: { path: { type: "string" }, content: { type: "string" } },
			required: ["path", "content"],
		},
		execute: async (_id, params) => {
			const p = resolve(params.path);
			await fs.mkdir(path.dirname(p), { recursive: true });
			await fs.writeFile(p, params.content, "utf8");
			return text(`wrote ${Buffer.byteLength(params.content)} bytes to ${p}`);
		},
	};

	return [terminal, processTool, readFile, writeFile];
}
