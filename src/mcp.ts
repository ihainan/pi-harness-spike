// One stdio McpClient per configured server, shared by all sessions in the process. Tools are
// wrapped per session so tools/call can carry _meta.zgOriginAcpSessionId. callTool() has no
// _meta option, hence client.request().
import * as fs from "node:fs";
import type { AgentTool } from "@earendil-works/pi-agent-core";
import { McpClient, StdioTransport, toLlmContent } from "@earendil-works/pi-mcp";
import { fault } from "./fault.js";
import { type OpRegistry, SerialQueue } from "./ops.js";

let firstSession: string | undefined;

interface ServerConfig {
	command: string;
	args?: string[];
	env?: Record<string, string>;
}

export interface McpHub {
	wrapTools(sessionId: string, ops: OpRegistry): AgentTool<any>[];
	close(): Promise<void>;
}

// A client-side timeout would end the wait while the single-threaded server is still busy, freeing the queue and the
// duplicate check for a call that is still running. ops.ts handles the wait instead; this only catches a dead connection.
const REQUEST_NEVER_MS = 7 * 24 * 3600 * 1000;

export async function connectMcp(configPath: string | undefined): Promise<McpHub> {
	const entries: { server: string; client: McpClient; tools: any[]; queue: SerialQueue }[] = [];
	if (configPath) {
		const cfg = JSON.parse(fs.readFileSync(configPath, "utf8")) as { mcpServers: Record<string, ServerConfig> };
		await Promise.all(
			Object.entries(cfg.mcpServers).map(async ([server, sc]) => {
				const client = new McpClient({ name: "pi-spike", version: "0.0.1", requestTimeoutMs: REQUEST_NEVER_MS });
				await client.connect(new StdioTransport({ command: sc.command, args: sc.args ?? [], env: sc.env }));
				entries.push({ server, client, tools: await client.listTools(), queue: new SerialQueue() });
			}),
		);
	}
	return {
		wrapTools(sessionId, ops) {
			firstSession ??= sessionId;
			const taken = new Set<string>();
			const out: AgentTool<any>[] = [];
			for (const { server, client, tools, queue } of entries) {
				for (const tool of tools) {
					// createMcpToolName/normalizeMcpInputSchema appear in the repo README but are not in the
					// published 1.5.0 package, so name and schema are normalized by hand.
					const name = `mcp__${server}__${tool.name}`.replace(/[^A-Za-z0-9_-]/g, "_").slice(0, 64);
					if (taken.has(name)) continue;
					taken.add(name);
					out.push({
						name,
						label: tool.title ?? tool.name,
						description: tool.description ?? tool.name,
						parameters: { ...tool.inputSchema, type: "object", properties: tool.inputSchema?.properties ?? {} },
						// The turn's signal is deliberately not passed down: aborting does not stop a platform MCP server (see ops.ts).
						execute: (toolCallId, params, signal) =>
							ops.run(
								{
									server,
									tool: tool.name,
									args: params,
									toolCallId,
									send: () => {
										const once = async () => {
											const result: any = await client.request(
												"tools/call",
												{
													name: tool.name,
													arguments: params,
													...(fault("no_meta") ? {} : { _meta: { zgOriginAcpSessionId: fault("wrong_sid") ? firstSession : sessionId } }),
												},
												{ timeoutMs: REQUEST_NEVER_MS },
											);
											return { content: toLlmContent(result), details: undefined, isError: result.isError === true };
										};
										if (fault("op_no_queue") || process.env.PI_MCP_CONCURRENT === "1") return { promise: once(), started: () => true, dropIfQueued: () => false };
										return queue.enqueue(once);
									},
								},
								signal,
							),
					});
				}
			}
			return out;
		},
		close: async () => {
			await Promise.allSettled(entries.map((e) => e.client.close()));
		},
	};
}
