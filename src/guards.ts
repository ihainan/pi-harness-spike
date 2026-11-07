// Model-layer repairs that Pi does not provide, each one forced by a failure seen on a real hub
// model (kit T7/T7x/T8). Kept separate so "how much code beyond Pi" stays measurable.
import type { AgentTurnContext } from "@earendil-works/pi-agent-core";
import { fault } from "./fault.js";

const OPEN = "<think>";
const CLOSE = "</think>";

/** gemma4 returns its reasoning inline in content. Splits a streamed reply; only a leading <think> counts. */
export class InlineThinkSplitter {
	private buf = "";
	private state: "start" | "think" | "text" = "start";

	feed(delta: string): { text: string; thought: string } {
		if (this.state === "text") return { text: delta, thought: "" };
		this.buf += delta;
		if (this.state === "start") {
			const trimmed = this.buf.trimStart();
			if (OPEN.startsWith(trimmed)) return { text: "", thought: "" }; // could still become the tag
			if (!trimmed.startsWith(OPEN)) return this.flushAsText();
			this.buf = trimmed.slice(OPEN.length);
			this.state = "think";
		}
		const end = this.buf.indexOf(CLOSE);
		if (end >= 0) {
			const thought = this.buf.slice(0, end);
			const rest = this.buf.slice(end + CLOSE.length).trimStart();
			this.buf = "";
			this.state = "text";
			return { text: rest, thought };
		}
		const keep = this.partialCloseLength();
		const thought = this.buf.slice(0, this.buf.length - keep);
		this.buf = this.buf.slice(this.buf.length - keep);
		return { text: "", thought };
	}

	private partialCloseLength(): number {
		for (let n = Math.min(CLOSE.length - 1, this.buf.length); n > 0; n--) {
			if (CLOSE.startsWith(this.buf.slice(-n))) return n;
		}
		return 0;
	}

	private flushAsText(): { text: string; thought: string } {
		const text = this.buf;
		this.buf = "";
		this.state = "text";
		return { text, thought: "" };
	}
}

/** Same rule on the final message, so <think> is neither replayed to the model nor persisted as text. */
export function stripInlineThink(message: any): void {
	if (message?.role !== "assistant" || !Array.isArray(message.content)) return;
	for (const block of message.content) {
		if (block.type !== "text") continue;
		const m = /^\s*<think>[\s\S]*?(<\/think>\s*|$)/.exec(block.text);
		if (m) block.text = block.text.slice(m[0].length);
	}
	message.content = message.content.filter((b: any) => b.type !== "text" || b.text !== "");
}

const MAX_FAILED_ROUNDS = 5;
// Repeating a successful call can be legitimate (kit T9c reads one file 12 times on request, and 5 here
// falsely stopped it), so this is only the far backstop; OpenClaw uses 20 as its critical threshold.
const MAX_SAME_CALL_ROUNDS = 20;
const MAX_ROUNDS = Number(process.env.PI_MAX_ROUNDS ?? 150);

/** qwen sent `write_file {}` 97 times in a row until the kit's quota stopped it; pi's loop has no such limit. */
export class ToolLoopGuard {
	private failed = 0;
	private same = 0;
	private rounds = 0;
	private last = "";
	tripped?: string;

	reset(): void {
		this.failed = this.same = this.rounds = 0;
		this.last = "";
		this.tripped = undefined;
	}

	check(turn: AgentTurnContext): { action: "end" } | undefined {
		if (fault("no_loop_guard")) return undefined; // kit T8L: see what the model does unguarded
		const calls = turn.message.content.filter((b: any) => b.type === "toolCall");
		if (calls.length === 0) return undefined;
		this.rounds++;
		const failed = turn.toolResults.length > 0 && turn.toolResults.every((r) => r.isError);
		this.failed = failed ? this.failed + 1 : 0;
		const sig = JSON.stringify(calls.map((b: any) => [b.name, b.arguments]));
		this.same = sig === this.last ? this.same + 1 : 0;
		this.last = sig;
		if (this.failed >= MAX_FAILED_ROUNDS) this.tripped = `${this.failed} consecutive failing tool rounds`;
		else if (this.same >= MAX_SAME_CALL_ROUNDS) this.tripped = `same tool call repeated ${this.same + 1} times`;
		else if (this.rounds >= MAX_ROUNDS) this.tripped = `${this.rounds} tool rounds in one turn`;
		return this.tripped ? { action: "end" } : undefined;
	}
}
