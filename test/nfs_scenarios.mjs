#!/usr/bin/env node
// Gate 0 #3 on real NFS. Every scenario checks one thing: whatever an actor said was acknowledged is readable, once and in
// order, by a third party that was not involved. "Loads without error" proves nothing here: a history can load perfectly
// and be missing acknowledged records.
//
//   Locally (checks the scenarios themselves, proves nothing about NFS):
//     NFS_DIR=/tmp/x node test/nfs_scenarios.mjs
//   Across clients, which is what counts: each actor must run on a different node, so a different kernel and NFS client.
//     NFS_DIR=/data/x ACTOR_X="kubectl exec -i pod-a -- node /p/test/nfs_actor.mjs" ACTOR_Y="..." ACTOR_Z="..." \
//     SIGNAL_X="kubectl exec pod-a -- kill" SIGNAL_Y="..." ROUNDS=50 node test/nfs_scenarios.mjs
import { spawn, spawnSync } from "node:child_process";
import * as path from "node:path";
import * as readline from "node:readline";
import { fileURLToPath } from "node:url";

const ROOT = process.env.NFS_DIR;
if (!ROOT) throw new Error("set NFS_DIR");
const ROUNDS = Number(process.env.ROUNDS ?? 5);
const local = `${process.execPath} ${fileURLToPath(new URL("./nfs_actor.mjs", import.meta.url))}`;
const only = process.env.ONLY?.split(",");
let uniq = `${process.pid}-${Date.now().toString(36)}`;
let seq = 0;

class Actor {
	constructor(name) {
		this.name = name;
		const cmd = process.env[`ACTOR_${name}`] ?? local;
		this.signalCmd = process.env[`SIGNAL_${name}`] ?? "kill";
		this.child = spawn(cmd, { shell: true, stdio: ["pipe", "pipe", "inherit"] });
		this.waiting = new Map();
		this.atWaiters = [];
		this.atSeen = [];
		this.child.stdin.on("error", () => {}); // Writing to a process a scenario just killed is expected, not a failure.
		this.closed = new Promise((r) => this.child.on("close", () => {
			this.gone = true;
			for (const w of this.waiting.values()) w({ ok: false, error: "the process is gone" });
			this.waiting.clear();
			r();
		}));
		this.ready = new Promise((r) => { this.onReady = r; });
		readline.createInterface({ input: this.child.stdout }).on("line", (line) => {
			const m = JSON.parse(line);
			if (m.pid) { this.pid = m.pid; this.onReady(); }
			else if (m.at) { const w = this.atWaiters.shift(); if (w) w(m.at); else this.atSeen.push(m.at); }
			else { this.waiting.get(m.id)?.(m); this.waiting.delete(m.id); }
		});
	}
	send(cmd, args = {}) {
		if (this.gone) return Promise.resolve({ ok: false, error: "the process is gone" });
		const id = ++seq;
		const p = new Promise((r) => this.waiting.set(id, r));
		this.child.stdin.write(`${JSON.stringify({ id, cmd, ...args })}\n`);
		return p;
	}
	async do(cmd, args) {
		const r = await this.send(cmd, args);
		if (!r.ok) throw new Error(`${this.name} ${cmd}: ${r.name}: ${r.error}`);
		return r;
	}
	frozenAt(limitMs = 20000) {
		if (this.atSeen.length) return Promise.resolve(this.atSeen.shift());
		return Promise.race([
			new Promise((r) => this.atWaiters.push(r)),
			new Promise((_, no) => setTimeout(() => no(new Error(`${this.name} never reached its pause point`)), limitMs)),
		]);
	}
	signal(sig) {
		const r = spawnSync(`${this.signalCmd} -${sig} ${this.pid}`, { shell: true, encoding: "utf8" });
		if (r.status !== 0) throw new Error(`signal ${sig} failed: ${r.stderr}`);
	}
	resume() { this.signal("CONT"); }
	async kill9() { this.signal("KILL"); this.child.kill("SIGKILL"); await this.closed; }
	async done() { try { this.child.stdin.end(); } catch {} this.child.kill("SIGKILL"); await this.closed; }
}

async function actors(...names) {
	const all = names.map((n) => new Actor(n));
	await Promise.all(all.map((a) => a.ready));
	return all;
}
function workspace(name, round) {
	const base = path.join(ROOT, `${uniq}-${name}-${round}`);
	return { dir: path.join(base, "state"), file: path.join(base, "s.jsonl") };
}
async function own(actor, dir) {
	await actor.do("claim", { dir });
	return actor.do("takeover", {});
}
/** The verdict. A fresh process with a fresh number is the next owner; it is what any later start would see. */
async function verify(w, acked, label, forbidden = []) {
	const [z] = await actors("Z");
	try {
		await own(z, w.dir);
		const { entries } = await z.do("open", { file: w.file });
		await z.do("close");
		const ids = entries.map((e) => e.i);
		const leaked = forbidden.filter((i) => ids.includes(i));
		if (leaked.length) throw new Error(`${label}: a write that ended after the seal is in the history: ${leaked.join(",")}`);
		const missing = acked.filter((i) => !ids.includes(i));
		if (missing.length) throw new Error(`${label}: acknowledged but gone: ${missing.join(",")}; history: ${ids.join(",")}`);
		if (new Set(ids).size !== ids.length) throw new Error(`${label}: duplicates: ${ids.join(",")}`);
		const order = ids.filter((i) => acked.includes(i));
		if (order.join() !== acked.join()) throw new Error(`${label}: out of order: ${order.join(",")} vs ${acked.join(",")}`);
	} finally {
		await z.done();
	}
}

const SCENARIOS = {
	// Review 7a #1: an opener frozen inside open() wakes up after someone else took over and acknowledged.
	async frozenOpener(round) {
		const w = workspace("opener", round);
		const [x, y, s] = await actors("X", "Y", "X");
		try {
			await own(s, w.dir);
			await s.do("open", { file: w.file });
			await s.do("append", { entry: { i: "seed" } });
			await s.do("close");
			await own(x, w.dir);
			await x.do("pause", { at: round % 2 ? "before-write" : "after-sync" });
			const opening = x.send("open", { file: w.file });
			await x.frozenAt();
			await own(y, w.dir);
			await y.do("open", { file: w.file });
			await y.do("append", { entry: { i: "R" } });
			x.resume();
			const opened = await opening;
			if (opened.ok) throw new Error("a replaced opener was told it succeeded");
			await verify(w, ["seed", "R"], "frozenOpener");
		} finally {
			await Promise.all([x, y, s].map((a) => a.done()));
		}
	},
	// Review 7a #2: three starters, with the lowest waking up last. The replaced writer must not come back.
	async threeStarters(round) {
		const w = workspace("three", round);
		const [l, a, b] = await actors("X", "Y", "X");
		try {
			await l.do("claim", { dir: w.dir });
			await own(a, w.dir);
			await a.do("open", { file: w.file });
			await a.do("append", { entry: { i: "A1" } });
			await own(b, w.dir);
			const late = await l.send("takeover", {});
			if (late.ok) throw new Error("the late starter believes it owns the directory");
			const after = await a.send("append", { entry: { i: "A2" } });
			if (after.ok) throw new Error("a replaced writer came back to life");
			await b.do("open", { file: w.file });
			await b.do("append", { entry: { i: "B1" } });
			await verify(w, ["A1", "B1"], "threeStarters");
		} finally {
			await Promise.all([l, a, b].map((c) => c.done()));
		}
	},
	// A writer frozen at each point of an append, replaced from another client, then woken up.
	async frozenWriter(round) {
		const at = ["before-check", "before-write", "after-write", "after-sync"][round % 4];
		const w = workspace("writer", round);
		const [x, y] = await actors("X", "Y");
		try {
			await own(x, w.dir);
			await x.do("open", { file: w.file });
			await x.do("append", { entry: { i: "A1" } });
			await x.do("pause", { at });
			const appending = x.send("append", { entry: { i: "A2" } });
			await x.frozenAt();
			await own(y, w.dir);
			await y.do("open", { file: w.file });
			await y.do("append", { entry: { i: "B1" } });
			x.resume();
			const r = await appending;
			if (r.ok) throw new Error(`frozen at ${at}: a replaced writer was told its write is safe`);
			// Frozen before the write, A2 lands after the new owner sealed the segment, so it must stay out of the history.
			await verify(w, ["A1", "B1"], `frozenWriter/${at}`, at === "before-write" ? ["A2"] : []);
		} finally {
			await Promise.all([x, y].map((a) => a.done()));
		}
	},
	// Review 8a #2 and #5: the client taking over has read this segment before, so it holds an old view of its size.
	async warmSegment(round) {
		const w = workspace("warm", round);
		const [x, y] = await actors("X", "Y");
		try {
			await own(y, w.dir);
			await y.do("open", { file: w.file });
			const seg = `${w.file}.g1`;
			await x.do("readSegment", { file: seg }); // X caches it short, right after creation.
			const acked = [];
			for (let i = 0; i < 20; i++) {
				const id = `Y${i}`;
				await y.do("append", { entry: { i: id, pad: "x".repeat(200) } });
				acked.push(id);
				if (i % 5 === 0) await x.do("readSegment", { file: seg });
			}
			await own(x, w.dir); // At once: no time for an attribute cache to expire.
			const { entries } = await x.do("open", { file: w.file });
			const seen = entries.map((e) => e.i);
			const missing = acked.filter((i) => !seen.includes(i));
			if (missing.length) throw new Error(`the taking-over client missed ${missing.length} of ${acked.length} acknowledged records`);
			await x.do("append", { entry: { i: "X1" } });
			await verify(w, [...acked, "X1"], "warmSegment");
		} finally {
			await Promise.all([x, y].map((a) => a.done()));
		}
	},
	// Review 8a #2: the taking-over client saw the segment while it was still empty.
	async warmEmptySegment(round) {
		const w = workspace("empty", round);
		const [x, y] = await actors("X", "Y");
		try {
			await own(y, w.dir);
			await y.do("pause", { at: "before-write" });
			const opening = y.send("open", { file: w.file });
			await y.frozenAt();
			await x.do("readSegment", { file: `${w.file}.g1` }); // Empty as far as X knows.
			y.resume();
			await (async () => { const o = await opening; if (!o.ok) throw new Error(o.error); })();
			await y.do("append", { entry: { i: "R" } });
			await own(x, w.dir);
			const { entries } = await x.do("open", { file: w.file });
			if (!entries.some((e) => e.i === "R")) throw new Error("the taking-over client skipped a segment it had seen empty");
			await verify(w, ["R"], "warmEmptySegment");
		} finally {
			await Promise.all([x, y].map((a) => a.done()));
		}
	},
	// Review 8a #3: this client cached "does not exist" for the segment before it existed. The hazard itself, tested on purpose.
	async warmNegative(round) {
		const w = workspace("neg", round);
		const [x, y] = await actors("X", "Y");
		try {
			await own(y, w.dir);
			await x.do("probe", { file: `${w.file}.g1` });
			await y.do("open", { file: w.file });
			await y.do("append", { entry: { i: "R" } });
			await own(x, w.dir);
			const { entries } = await x.do("open", { file: w.file });
			if (!entries.some((e) => e.i === "R")) throw new Error("a cached 'does not exist' hid an acknowledged segment");
			await verify(w, ["R"], "warmNegative");
		} finally {
			await Promise.all([x, y].map((a) => a.done()));
		}
	},
	// SIGKILL under a real fence, replaced from another client. The earlier kill test had no fence and one client.
	async killAcrossClients(round) {
		const w = workspace("kill", round);
		const acked = [];
		const [x] = await actors("X");
		await own(x, w.dir);
		await x.do("open", { file: w.file });
		const writing = (async () => {
			for (let i = 0; ; i++) {
				const r = await x.send("append", { entry: { i: `K${i}`, pad: "第".repeat(500) } });
				if (!r.ok) break;
				acked.push(`K${i}`);
			}
		})();
		await new Promise((r) => setTimeout(r, 50 + (round % 7) * 40));
		await x.kill9();
		await writing;
		if (!acked.length) throw new Error("the writer never got a record out, so this round proved nothing");
		await verify(w, [...acked], "killAcrossClients");
	},
};

let failed = 0;
for (const [name, fn] of Object.entries(SCENARIOS)) {
	if (only && !only.includes(name)) continue;
	let ok = 0;
	const errors = [];
	const t0 = Date.now();
	for (let round = 0; round < ROUNDS; round++) {
		try {
			await fn(round);
			ok++;
		} catch (err) {
			errors.push(String(err?.message ?? err));
		}
	}
	console.log(JSON.stringify({ scenario: name, rounds: ROUNDS, ok, ms: Date.now() - t0, errors: errors.slice(0, 3) }));
	if (ok !== ROUNDS) failed++;
}
process.exit(failed ? 1 : 0);
