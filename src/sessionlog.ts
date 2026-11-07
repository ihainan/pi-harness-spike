// Append-only session log. One record per line:
//   {"v":1,"seq":<n>,"crc":"<crc32 of the payload bytes>","e":<payload>}\n
//
// A session is a backward-linked chain of segments, one per process generation: <base>.g<n>. A process only ever writes
// to its own segment, and nothing is ever cut from any file. Opening a session writes a new segment whose header records
// the previous segment and how many of its bytes are valid. So:
//   - A torn tail needs no repair: it is past the recorded length, and stays in the file as evidence.
//   - A process that was paused between its fence check and its write can still write, but only into its own old
//     segment, past the sealed length, where nobody reads. No wait time can close that window; this makes it harmless.
//     It re-checks the fence after writing, so such a write is not acknowledged and the side effect behind it is not sent.
//   - The newest segment is the highest number with an intact header, found by trying names downward from the owner's own
//     generation. There is no shared "latest" pointer: an earlier head file could be written back by a process waking up
//     late, which cut acknowledged records off the chain, and no check before that write closes the window. A late process
//     can only add a lower number here, and the chain walks past it.
//   - A crash while opening leaves a segment with a torn header, which is skipped, or an intact header with no records,
//     which is an empty and correct link in the chain.
//   - Names are tried one by one, never found by listing the directory: on NFS a cached listing can miss the newest
//     segment, and a process that believes that links its history past someone else's records. An owner looks only below
//     its own number, after revoking everything below it. So a "does not exist" that another owner cached on this client
//     was for a number already revoked by then, which can no longer acknowledge anything. Only a process outside this
//     protocol, looking up names in advance, could cache one that matters; read() goes by the listing for that reason.
//   - Acknowledged means readable by the next owner only with a real fence as the guard. Without one (tests of the
//     isolation alone) a replaced writer is told its write succeeded, into a segment nobody reads.
//   - What this does not settle by itself, and real NFS must: that a fresh open sees every byte another client synced.
//     A stale view of the newest segment would seal it short, or skip it if it looks empty. See nfs_scenarios.mjs.
//   - Damage that is not a tail is an error. Silently skipping a middle record would hand the model a history with a
//     hole in it, which is worse than refusing to load.
// The last complete line cannot be told apart from a tear if it is damaged, so it is treated as one. This log promises to
// survive process crashes and replacement; it does not promise to notice bit rot in its last synced record.
import * as fs from "node:fs";
import * as path from "node:path";
import { crc32 } from "node:zlib";
import { fault } from "./fault.js";

export class SessionCorruptError extends Error {
	readonly offset: number;
	constructor(file: string, offset: number, why: string) {
		super(`session log corrupt: ${file} at byte ${offset}: ${why}`);
		this.name = "SessionCorruptError";
		this.offset = offset;
	}
}

export class SessionWriteError extends Error {
	constructor(message: string) {
		super(message);
		this.name = "SessionWriteError";
	}
}

/** The syscalls a failing disk can break; tests replace them to inject short writes and errors. */
export interface LogIo {
	writeSync: (fd: number, buf: Uint8Array, off: number, len: number) => number;
	fsyncSync: (fd: number) => void;
}

export interface OpenOptions {
	fsync?: boolean;
	io?: Partial<LogIo>;
	/** Runs before every write and throws if this process may no longer write. */
	guard?: () => void;
	/** The owner's generation; names the new segment. Without it the next free number is used. */
	generation?: number;
}

export interface Repair {
	offset: number;
	droppedBytes: number;
}

const NL = 0x0a;
const PREFIX = /^\{"v":1,"seq":(\d{1,15}),"crc":"([0-9a-f]{8})","e":/;
const PREFIX_SCAN = 64;

function crcHex(buf: Uint8Array): string {
	return crc32(buf).toString(16).padStart(8, "0");
}

export function encodeRecord(seq: number, entry: unknown): Buffer {
	const payload = Buffer.from(JSON.stringify(entry), "utf8");
	return Buffer.concat([Buffer.from(`{"v":1,"seq":${seq},"crc":"${crcHex(payload)}","e":`, "latin1"), payload, Buffer.from("}\n", "latin1")]);
}

/** Decodes one newline-terminated line, or returns undefined if it is not an intact record. */
function decodeLine(line: Buffer): { seq: number; entry: unknown } | undefined {
	const m = PREFIX.exec(line.toString("latin1", 0, Math.min(PREFIX_SCAN, line.length)));
	if (!m || line.length < m[0].length + 2 || line[line.length - 2] !== 0x7d) return undefined;
	const payload = line.subarray(m[0].length, line.length - 2);
	if (!fault("log_no_crc") && crcHex(payload) !== m[2]) return undefined;
	try {
		return { seq: Number(m[1]), entry: JSON.parse(payload.toString("utf8")) };
	} catch {
		return undefined;
	}
}

interface Scan {
	entries: unknown[];
	nextSeq: number;
	goodBytes: number;
	repair?: Repair;
}

function scan(file: string, buf: Buffer): Scan {
	const skipCorrupt = fault("log_skip_corrupt"); // Negative control: the old "skip what does not parse" behaviour.
	const entries: unknown[] = [];
	let nextSeq = 1;
	let goodBytes = 0;
	let pos = 0;
	let badLines = 0;
	let firstBad = -1;
	while (pos < buf.length) {
		const nl = buf.indexOf(NL, pos);
		if (nl < 0) break; // Unterminated fragment: handled as the tail below.
		const rec = decodeLine(buf.subarray(pos, nl + 1));
		if (!rec) {
			if (firstBad < 0) firstBad = pos;
			badLines++;
		} else if (skipCorrupt) {
			entries.push(rec.entry);
			nextSeq = rec.seq + 1;
			firstBad = -1;
			badLines = 0;
			goodBytes = nl + 1;
		} else {
			if (firstBad >= 0) throw new SessionCorruptError(file, firstBad, "damaged record followed by an intact one");
			if (rec.seq !== nextSeq && !fault("log_no_seq")) throw new SessionCorruptError(file, pos, `expected seq ${nextSeq}, found ${rec.seq}`);
			entries.push(rec.entry);
			nextSeq = rec.seq + 1;
			goodBytes = nl + 1;
		}
		pos = nl + 1;
	}
	// A crash tears one write. Several damaged lines at the end are not a tear, so refuse rather than cut them.
	if (badLines > 1 && !skipCorrupt) throw new SessionCorruptError(file, firstBad, `${badLines} damaged records at the end`);
	if (goodBytes === buf.length) return { entries, nextSeq, goodBytes };
	return { entries, nextSeq, goodBytes, repair: { offset: goodBytes, droppedBytes: buf.length - goodBytes } };
}

function markerFile(base: string): string {
	return `${base}.created`;
}

/** Opening a directory revalidates it, which drops any "does not exist" this client cached for names inside it. */
function revalidateDir(base: string): void {
	try {
		fs.opendirSync(path.dirname(base)).closeSync();
	} catch (err: any) {
		if (err?.code !== "ENOENT") throw err;
	}
}

function segmentNumbers(base: string): number[] {
	const dir = path.dirname(base);
	const prefix = `${path.basename(base)}.g`;
	return (fs.existsSync(dir) ? fs.readdirSync(dir) : [])
		.filter((name) => name.startsWith(prefix) && Number.isInteger(Number(name.slice(prefix.length))))
		.map((name) => Number(name.slice(prefix.length)))
		.sort((x, y) => x - y);
}

/** False for a segment a crash during open left behind, which holds nothing anyone was told about. */
function hasStarted(base: string, gen: number): boolean {
	let buf: Buffer;
	try {
		buf = fs.readFileSync(`${base}.g${gen}`);
	} catch (err: any) {
		if (err?.code === "ENOENT") return false;
		throw err;
	}
	const nl = buf.indexOf(NL);
	if (nl < 0) return false; // Empty, or a header that never finished.
	if (nl === buf.length - 1 && !decodeLine(buf)) return false; // One damaged line and nothing after it: a tear.
	return true; // Anything else is either fine or damage, and reading the chain tells which.
}

/**
 * The newest segment of a session, or 0. An owner tries names below its own generation. Without one (tools that only look,
 * and tests) the directory listing is the only source there is.
 */
function findNewest(base: string, below?: number): number {
	if (fault("log_latest_opener")) {
		// Negative control: whoever wrote last decides, which is what a shared pointer amounts to.
		const all = segmentNumbers(base).filter((g) => hasStarted(base, g));
		all.sort((x, y) => fs.statSync(`${base}.g${x}`).mtimeMs - fs.statSync(`${base}.g${y}`).mtimeMs);
		return all.at(-1) ?? 0;
	}
	if (below === undefined) {
		for (const g of segmentNumbers(base).reverse()) if (hasStarted(base, g)) return g;
		return 0;
	}
	revalidateDir(base);
	for (let g = below - 1; g > 0; g--) if (hasStarted(base, g)) return g;
	return 0;
}

/** Follows the chain back from the newest segment. The length a later segment recorded decides how much of an earlier one counts. */
function readChain(base: string, below?: number): { entries: unknown[]; lastGen: number; lastValid: number; repair?: Repair } {
	const head = findNewest(base, below);
	if (head === 0) return { entries: [], lastGen: 0, lastValid: 0 };
	const groups: unknown[][] = [];
	let gen = head;
	let limit: number | undefined;
	let lastValid = 0;
	let repair: Repair | undefined;
	while (gen > 0) {
		const file = `${base}.g${gen}`;
		let buf: Buffer;
		try {
			buf = fs.readFileSync(file);
		} catch (err: any) {
			if (err?.code === "ENOENT") throw new SessionCorruptError(file, 0, "a segment the chain needs is missing");
			throw err;
		}
		const sealed = limit !== undefined && !fault("log_no_seal");
		if (sealed && limit! > buf.length) throw new SessionCorruptError(file, buf.length, `sealed at ${limit} bytes, only ${buf.length} present`);
		const scanned = scan(file, sealed ? buf.subarray(0, limit) : buf);
		if (scanned.repair && limit !== undefined) throw new SessionCorruptError(file, scanned.repair.offset, "damage inside the sealed part");
		const header = scanned.entries[0] as any;
		if (header?.t !== "seg" || header.gen !== gen) throw new SessionCorruptError(file, 0, "segment has no valid header");
		if (gen === head) {
			lastValid = scanned.goodBytes;
			repair = scanned.repair;
		}
		groups.unshift(scanned.entries.slice(1));
		if (header.prevGen !== 0 && header.prevGen >= gen) throw new SessionCorruptError(file, 0, `chain goes forward to ${header.prevGen}`);
		limit = header.prevLen;
		gen = header.prevGen;
	}
	return { entries: groups.flat(), lastGen: head, lastValid, repair };
}

export class SessionLog {
	readonly file: string;
	readonly entries: unknown[];
	readonly repair?: Repair;
	private fd: number;
	private nextSeq = 1;
	private size = 0;
	private failed?: string;
	private useFsync: boolean;
	private io: LogIo;
	private guard?: () => void;

	private constructor(file: string, fd: number, entries: unknown[], repair: Repair | undefined, opts: OpenOptions) {
		this.file = file;
		this.fd = fd;
		this.entries = entries;
		this.repair = repair;
		this.useFsync = opts.fsync ?? true;
		this.guard = opts.guard;
		this.io = {
			writeSync: opts.io?.writeSync ?? ((f, b, o, l) => fs.writeSync(f, b, o, l)),
			fsyncSync: opts.io?.fsyncSync ?? fs.fsyncSync,
		};
	}

	static exists(base: string): boolean {
		revalidateDir(base);
		return fs.existsSync(markerFile(base));
	}

	/** Every segment file present, chained or not. For looking at evidence; the history comes from read(). */
	static segments(base: string): string[] {
		return segmentNumbers(base).map((g) => `${base}.g${g}`);
	}

	/** The history as it stands, for looking only: no segment is created and nothing is sealed. */
	static read(base: string): { entries: unknown[]; repair?: Repair } {
		const { entries, repair } = readChain(base);
		return { entries, repair };
	}

	/** Verifies the chain and starts this process's segment. Throws SessionCorruptError without changing the chain. */
	static open(base: string, opts: OpenOptions = {}): SessionLog {
		opts.guard?.();
		const chain = readChain(base, opts.generation);
		let gen = opts.generation ?? chain.lastGen + 1;
		let fd: number;
		for (;;) {
			try {
				fd = fs.openSync(`${base}.g${gen}`, fs.constants.O_RDWR | fs.constants.O_CREAT | fs.constants.O_EXCL | fs.constants.O_APPEND, 0o600);
				break;
			} catch (err: any) {
				if (err?.code !== "EEXIST") throw err;
				// A fence's number is unique, so a clash there means this session was already opened in this process.
				if (opts.generation !== undefined) throw new SessionWriteError(`${base}: generation ${gen} already has a segment`);
				gen++; // Without a fence a leftover from a crash during open can hold the number.
			}
		}
		const log = new SessionLog(`${base}.g${gen}`, fd, chain.entries, chain.repair, opts);
		try {
			log.appendRecord({ t: "seg", gen, prevGen: chain.lastGen, prevLen: chain.lastValid, dropped: chain.repair?.droppedBytes ?? 0, at: Date.now() });
			// Says only that this session exists. Written once and never changed, so nothing can write it back.
			fs.closeSync(fs.openSync(markerFile(base), fs.constants.O_WRONLY | fs.constants.O_CREAT, 0o600));
			return log;
		} catch (err) {
			fs.closeSync(fd);
			throw err;
		}
	}

	/** Durable once it returns. After a failure the log refuses further writes rather than record a history with a gap. */
	append(entry: unknown): void {
		this.appendRecord(entry);
		this.entries.push(entry);
	}

	private appendRecord(entry: unknown): void {
		if (this.failed) throw new SessionWriteError(this.failed);
		if (this.guard) {
			try {
				this.guard();
			} catch (err: any) {
				this.failed = err?.message ?? String(err);
				throw err;
			}
		}
		if (!fault("log_no_size_check")) {
			const actual = fs.fstatSync(this.fd).size;
			if (actual !== this.size) {
				this.failed = `another writer changed ${this.file}: expected ${this.size} bytes, found ${actual}`;
				throw new SessionWriteError(this.failed);
			}
		}
		const buf = encodeRecord(this.nextSeq, entry);
		try {
			let off = 0;
			while (off < buf.length) off += this.io.writeSync(this.fd, buf, off, buf.length - off);
			if (this.useFsync && !fault("log_no_fsync")) this.io.fsyncSync(this.fd);
		} catch (err: any) {
			// No truncating back: a new owner may already have counted these bytes as part of the chain. A partial record just
			// becomes a tail for the next reader. A whole one that failed to sync may survive, but it was never acknowledged.
			this.failed = `write failed on ${this.file}: ${err?.message ?? err}`;
			throw new SessionWriteError(this.failed);
		}
		if (this.guard && !fault("log_no_post_guard")) {
			// Replaced while writing: this record may lie past the seal, so nothing may act as if it were durable.
			try {
				this.guard();
			} catch (err: any) {
				this.failed = err?.message ?? String(err);
				throw err;
			}
		}
		this.size += buf.length;
		this.nextSeq++;
	}

	close(): void {
		fs.closeSync(this.fd);
	}
}

const INTERRUPTED = "[工具执行被中断：进程在工具返回前退出，结果未知。不要假设它已成功或已失败；如需重试，先确认原操作是否已经生效。]";

/**
 * A crash between a tool call and its result leaves a transcript the model API refuses, and re-running the tool
 * could repeat a side effect. So each unanswered call gets a result that says the outcome is unknown; nothing is
 * re-executed. Returns the new messages to persist.
 */
export function repairTranscript(messages: any[], known: Map<string, { text: string; isError: boolean }> = new Map()): { messages: any[]; added: any[] } {
	if (fault("log_no_tool_repair")) return { messages, added: [] };
	const answered = new Set(messages.filter((m) => m.role === "toolResult").map((m) => m.toolCallId));
	const out: any[] = [];
	const added: any[] = [];
	for (let i = 0; i < messages.length; i++) {
		out.push(messages[i]);
		if (messages[i].role !== "assistant") continue;
		const missing = (messages[i].content ?? []).filter((b: any) => b.type === "toolCall" && !answered.has(b.id));
		if (!missing.length) continue;
		while (messages[i + 1]?.role === "toolResult") out.push(messages[++i]);
		for (const call of missing) {
			// A real result may be on record even though the crash took the message carrying it. It beats "unknown".
			const real = fault("op_no_known_result") ? undefined : known.get(call.id);
			const m = real
				? { role: "toolResult", toolCallId: call.id, toolName: call.name, content: [{ type: "text", text: real.text }], isError: real.isError, timestamp: Date.now(), details: { recovered: true } }
				: { role: "toolResult", toolCallId: call.id, toolName: call.name, content: [{ type: "text", text: INTERRUPTED }], isError: true, timestamp: Date.now(), details: { interrupted: true } };
			out.push(m);
			added.push(m);
		}
	}
	return { messages: out, added };
}

/** Folds log entries into the current transcript. */
export function replayEntries(entries: any[]): { cwd?: string; messages: any[] } {
	let messages: any[] = [];
	let cwd: string | undefined;
	for (const e of entries) {
		if (e.t === "hdr") cwd = e.cwd;
		else if (e.t === "msg") messages.push(e.m);
		else if (e.t === "compact") messages = e.messages;
		else if (e.t === "drop_last") messages.pop();
	}
	return { cwd, messages };
}
