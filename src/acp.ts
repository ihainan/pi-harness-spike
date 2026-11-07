// Minimal ACP (JSON-RPC 2.0, ndjson over stdio) peer. Requests run concurrently: a
// session/prompt stays in flight while /steer or session/new on the same pipe complete.
import * as readline from "node:readline";

export class RpcError extends Error {
	constructor(public code: number, message: string, public data?: unknown) {
		super(message);
	}
}

export type Handler = (params: any) => Promise<unknown> | unknown;

export class AcpPeer {
	private handlers = new Map<string, Handler>();

	on(method: string, handler: Handler): void {
		this.handlers.set(method, handler);
	}

	notify(method: string, params: unknown): void {
		this.write({ jsonrpc: "2.0", method, params });
	}

	listen(onClose: () => void): void {
		const rl = readline.createInterface({ input: process.stdin, crlfDelay: Infinity });
		rl.on("line", (line) => {
			if (line.trim()) this.dispatch(line);
		});
		rl.on("close", onClose);
	}

	private write(obj: unknown): void {
		process.stdout.write(`${JSON.stringify(obj)}\n`);
	}

	private async dispatch(line: string): Promise<void> {
		let msg: any;
		try {
			msg = JSON.parse(line);
		} catch {
			this.write({ jsonrpc: "2.0", id: null, error: { code: -32700, message: "parse error" } });
			return;
		}
		if (msg.method === undefined) return; // responses to agent->client requests: we send none
		const handler = this.handlers.get(msg.method);
		const isRequest = msg.id !== undefined;
		if (!handler) {
			if (isRequest) {
				this.write({ jsonrpc: "2.0", id: msg.id, error: { code: -32601, message: `method not found: ${msg.method}` } });
			}
			return;
		}
		try {
			const result = await handler(msg.params ?? {});
			if (isRequest) this.write({ jsonrpc: "2.0", id: msg.id, result: result ?? null });
		} catch (err: any) {
			if (isRequest) {
				const code = err instanceof RpcError ? err.code : -32000;
				this.write({ jsonrpc: "2.0", id: msg.id, error: { code, message: String(err?.message ?? err), data: err?.data } });
			} else {
				console.error("notification handler failed", err);
			}
		}
	}
}
