// Builds the single hub model and a Models instance from KIT_* env. No catalog entry is
// used: the upstream is an OpenAI-compatible gateway, so pi's URL-based compat autodetect sees an unknown
// endpoint and every provider quirk must be declared in KIT_COMPAT.
import { createModels, createProvider, type Model } from "@earendil-works/pi-ai";
import { openAICompletionsApi } from "@earendil-works/pi-ai/api/openai-completions.lazy";

export interface HarnessConfig {
	model: Model<any>;
	models: ReturnType<typeof createModels>;
	thinkingLevel: "off" | "minimal" | "low" | "medium" | "high";
	compactRatio: number;
	stateDir: string;
	mcpConfigPath?: string;
	mcpTimeoutMs: number;
	/** Compaction summary model; a fast helper with thinking off can be used instead of the main model. */
	auxModel?: Model<any>;
}

function env(name: string, fallback?: string): string {
	const v = process.env[name] ?? fallback;
	if (v === undefined) throw new Error(`missing env ${name}`);
	return v;
}

export function loadConfig(): HarnessConfig {
	const apiKey = env("KIT_API_KEY");
	const baseUrl = env("KIT_BASE_URL");
	const model = {
		id: env("KIT_MODEL"),
		name: env("KIT_MODEL"),
		api: "openai-completions",
		provider: "hub",
		baseUrl,
		reasoning: env("KIT_REASONING", "false") === "true",
		input: ["text"],
		cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
		contextWindow: Number(env("KIT_CONTEXT_WINDOW", "128000")),
		maxTokens: Number(env("KIT_MAX_TOKENS", "8192")),
		compat: JSON.parse(env("KIT_COMPAT", "{}")),
	} as Model<any>;
	const models = createModels();
	models.setProvider(
		createProvider({
			id: "hub",
			baseUrl,
			// ProviderAuth has no "static key" shorthand; resolve() is the smallest form.
			auth: { apiKey: { name: "hub", resolve: async () => ({ auth: { apiKey }, source: "KIT_API_KEY" }) } },
			models: [model],
			api: openAICompletionsApi(),
		}),
	);
	const auxId = process.env.PI_AUX_MODEL;
	const auxFormat = process.env.PI_AUX_THINKING_FORMAT ?? "qwen-chat-template";
	const auxModel = auxId
		? ({ ...model, id: auxId, name: auxId, reasoning: true, compat: { thinkingFormat: auxFormat }, maxTokens: 4096 } as Model<any>)
		: undefined;
	return {
		model,
		auxModel,
		models,
		thinkingLevel: env("KIT_THINKING", "off") as HarnessConfig["thinkingLevel"],
		compactRatio: Number(env("KIT_COMPACT_RATIO", "0.75")),
		stateDir: env("KIT_STATE_DIR", "/tmp/pi-harness"),
		mcpConfigPath: process.env.KIT_MCP_CONFIG,
		mcpTimeoutMs: Number(env("KIT_MCP_TIMEOUT_MS", String(2 * 3600 * 1000))),
	};
}
