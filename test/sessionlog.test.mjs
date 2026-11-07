// Gate 0 #3. LOG_DIR selects the filesystem, so the same file runs on local disk and on a real NFS mount.
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { test } from "node:test";
import { fileURLToPath } from "node:url";
import { Fence, FencedError, writeAll } from "../dist/fence.js";
import { knownResults, reconcileOps } from "../dist/ops.js";
import { repairTranscript, replayEntries, SessionCorruptError, SessionLog, SessionWriteError } from "../dist/sessionlog.js";

const ROOT = fs.mkdtempSync(path.join(process.env.LOG_DIR ?? os.tmpdir(), "sessionlog-"));
process.on("exit", () => fs.rmSync(ROOT, { recursive: true, force: true }));
let n = 0;
const fresh = () => path.join(ROOT, `s${n++}.jsonl`);
// Multi-byte text so that truncation lands inside a UTF-8 sequence, and a newline inside a value.
const rec = (i) => ({ t: "msg", i, text: `第${i}条消息 🙂 line\nbreak`, pad: "x".repeat(i % 5) });
const lastSeg = (base) => SessionLog.segments(base).at(-1);

function build(count, file = fresh()) {
	const log = SessionLog.open(file);
	for (let i = 0; i < count; i++) log.append(rec(i));
	log.close();
	return file;
}
const look = (file) => SessionLog.read(file);
// A stored session as a crash could have left it: one segment.
function plant(bytes, file = fresh()) {
	fs.writeFileSync(`${file}.g1`, bytes);
	fs.writeFileSync(`${file}.created`, "");
	return file;
}

test("torn at every byte: recover -> append -> recover keeps every complete record", () => {
	const whole = fs.readFileSync(lastSeg(build(6)));
	const ends = [];
	for (let i = 0; i < whole.length; i++) if (whole[i] === 0x0a) ends.push(i + 1);
	for (let cut = ends[0]; cut < whole.length; cut++) {
		const file = plant(whole.subarray(0, cut));
		const complete = ends.filter((e) => e <= cut).length - 1; // The first line is the segment header.
		const first = SessionLog.open(file);
		assert.deepEqual(first.entries, Array.from({ length: complete }, (_, i) => rec(i)), `cut=${cut}`);
		assert.equal(first.repair !== undefined, !ends.includes(cut), `cut=${cut} repair flag`);
		first.append(rec(100));
		first.close();
		const second = look(file);
		assert.equal(second.repair, undefined, `cut=${cut}: the next reader must not see a tear`);
		assert.deepEqual(second.entries, [...Array.from({ length: complete }, (_, i) => rec(i)), rec(100)], `cut=${cut} after append`);
		const log = SessionLog.open(file);
		log.append(rec(101));
		log.close();
		assert.deepEqual(look(file).entries.at(-1), rec(101), `cut=${cut} second append`);
		assert.equal(fs.statSync(`${file}.g1`).size, cut, `cut=${cut}: an old segment is never changed, the tail stays as evidence`);
	}
});

test("a crash while opening leaves nothing the next reader can trip over", () => {
	const file = build(2);
	const before = look(file).entries;
	const torn = SessionLog.segments(file).length + 1;
	fs.writeFileSync(`${file}.g${torn}`, '{"v":1,"seq":1,"crc":"0000');
	fs.appendFileSync(lastSeg(file), "half a rec"); // The tail of the old segment is torn as well.
	const log = SessionLog.open(file);
	assert.deepEqual(log.entries, before);
	log.append(rec(7));
	log.close();
	assert.deepEqual(look(file).entries, [...before, rec(7)]);
});

test("a missing segment refuses to load, even at the start of the chain", () => {
	const file = build(1);
	for (let i = 0; i < 2; i++) { const l = SessionLog.open(file); l.append(rec(i + 10)); l.close(); }
	const [first, second] = SessionLog.segments(file);
	const saved = fs.readFileSync(first);
	fs.rmSync(first);
	assert.throws(() => look(file), SessionCorruptError, "root missing");
	fs.writeFileSync(first, saved);
	fs.rmSync(second);
	assert.throws(() => look(file), SessionCorruptError, "middle missing");
});

test("an owner finds the chain by name, so a segment outside its range does not count", () => {
	const file = fresh();
	const one = SessionLog.open(file, { generation: 2 });
	one.append(rec(1));
	one.close();
	// A higher number can only belong to a newer owner, in which case this process is about to be fenced anyway.
	const higher = SessionLog.open(`${file}.other`, { generation: 9 });
	higher.append(rec(99));
	higher.close();
	fs.copyFileSync(lastSeg(`${file}.other`), `${file}.g9`);
	const owner = SessionLog.open(file, { generation: 5 });
	assert.deepEqual(owner.entries, [rec(1)]);
	owner.close();
	assert.throws(() => SessionLog.open(file, { generation: 5 }), SessionWriteError, "one generation, one segment");
});

test("a late opener cannot cut what the new owner acknowledged off the chain", () => {
	// Review 7a, #1. A has its number and is frozen before it writes anything; B takes over and acknowledges R; A wakes up.
	const file = fresh();
	const seed = SessionLog.open(file, { generation: 1 });
	seed.append(rec(0));
	seed.close();
	const b = SessionLog.open(file, { generation: 3 });
	b.append(rec(1)); // R.
	b.close();
	const a = SessionLog.open(file, { generation: 2 });
	assert.deepEqual(a.entries, [rec(0)], "A does not see R, which is why it must not get to decide anything");
	a.append(rec(2)); // No fence in this test: this is only the isolation half. The fence half is below.
	a.close();
	const next = SessionLog.open(file, { generation: 4 });
	assert.deepEqual(next.entries, [rec(0), rec(1)]);
	next.close();
	assert.deepEqual(look(file).entries, [rec(0), rec(1)]);
});

test("a crash while opening under a fence is skipped, torn or not", () => {
	const file = fresh();
	const a = SessionLog.open(file, { generation: 1 });
	a.append(rec(0));
	a.close();
	fs.writeFileSync(`${file}.g2`, '{"v":1,"seq":1,"crc":"0000'); // Torn header.
	fs.writeFileSync(`${file}.g3`, ""); // Created, nothing written.
	const c = SessionLog.open(file, { generation: 4 }); // An intact header with no records is a correct empty link.
	assert.deepEqual(c.entries, [rec(0)]);
	c.close();
	const d = SessionLog.open(file, { generation: 5 });
	d.append(rec(1));
	d.close();
	assert.deepEqual(SessionLog.open(file, { generation: 6 }).entries, [rec(0), rec(1)]);
});

test("garbage tails are left behind: NUL padding, random bytes, an invalid terminated last line", () => {
	for (const tail of [Buffer.alloc(4096), Buffer.from([0xff, 0xfe, 0x7b, 0x22]), Buffer.from('{"v":1,"seq":5,"crc":"00000000","e":{}}\n'), Buffer.from('{"t":"legacy"}\n')]) {
		const file = build(3);
		fs.appendFileSync(lastSeg(file), tail);
		const log = SessionLog.open(file);
		assert.equal(log.repair?.droppedBytes, tail.length);
		assert.deepEqual(log.entries, [rec(0), rec(1), rec(2)]);
		log.append(rec(9));
		log.close();
		assert.deepEqual(look(file).entries, [rec(0), rec(1), rec(2), rec(9)]);
	}
});

test("any single bit flipped in a middle record refuses to load and leaves the file untouched", () => {
	const whole = fs.readFileSync(lastSeg(build(3)));
	const secondEnd = whole.indexOf(0x0a, whole.indexOf(0x0a) + 1);
	for (let i = 0; i < secondEnd; i += 3) {
		for (const bit of [0x01, 0x10]) {
			const mutated = Buffer.from(whole);
			mutated[i] ^= bit;
			if (mutated[i] === 0x0a) continue; // That adds a line break, covered by the seq and tear checks.
			const file = plant(mutated);
			assert.throws(() => SessionLog.open(file), SessionCorruptError, `flip byte ${i} bit ${bit}`);
			assert.deepEqual(fs.readFileSync(`${file}.g1`), mutated, `flip byte ${i} changed the file`);
			assert.equal(SessionLog.segments(file).length, 1, "a refused open must not start a segment");
		}
	}
});

test("damage inside a sealed segment refuses to load even at its very end", () => {
	const file = build(2);
	const log = SessionLog.open(file);
	log.append(rec(5));
	log.close();
	const first = SessionLog.segments(file)[0];
	const b = fs.readFileSync(first);
	b[b.length - 5] ^= 0x01;
	fs.writeFileSync(first, b);
	assert.throws(() => look(file), SessionCorruptError);
});

test("duplicated and missing records are refused", () => {
	const lines = fs.readFileSync(lastSeg(build(3)), "latin1").split("\n").slice(0, 4);
	const dup = plant(Buffer.from(`${lines[0]}\n${lines[1]}\n${lines[1]}\n${lines[2]}\n`, "latin1"));
	assert.throws(() => SessionLog.open(dup), SessionCorruptError);
	const gap = plant(Buffer.from(`${lines[0]}\n${lines[2]}\n${lines[3]}\n`, "latin1"));
	assert.throws(() => SessionLog.open(gap), SessionCorruptError);
});

test("short writes complete the record", () => {
	const file = fresh();
	const log = SessionLog.open(file, { io: { writeSync: (f, b, o, l) => fs.writeSync(f, b, o, Math.min(l, 7)) } });
	log.append(rec(1));
	log.close();
	assert.deepEqual(look(file).entries, [rec(1)]);
});

test("a failed write stops the log, leaves the file alone, and the next reader only sees a tail", () => {
	const file = build(2);
	let armed = false;
	let calls = 0;
	const log = SessionLog.open(file, {
		io: {
			writeSync: (f, b, o, l) => {
				if (!armed) return fs.writeSync(f, b, o, l);
				if (++calls === 1) return fs.writeSync(f, b, o, 5);
				throw Object.assign(new Error("ENOSPC"), { code: "ENOSPC" });
			},
		},
	});
	armed = true;
	assert.throws(() => log.append(rec(3)), SessionWriteError);
	assert.throws(() => log.append(rec(4)), SessionWriteError);
	log.close();
	assert.deepEqual(look(file).entries, [rec(0), rec(1)]);
	assert.equal(look(file).repair?.droppedBytes, 5);
});

test("a failed sync never takes back bytes a new owner may have counted", () => {
	// The old process's record reached the file, the new owner sealed past it and wrote, then the old sync failed.
	const file = build(1);
	let armed = false;
	const old = SessionLog.open(file, {
		io: {
			fsyncSync: (fd) => {
				if (!armed) return fs.fsyncSync(fd);
				armed = false;
				const owner = SessionLog.open(file);
				owner.append(rec(60));
				owner.close();
				throw new Error("EIO");
			},
		},
	});
	armed = true;
	assert.throws(() => old.append(rec(50)), SessionWriteError);
	old.close();
	const { entries } = look(file);
	assert.deepEqual(entries.map((e) => e.i), [0, 50, 60], "the chain must still load");
});

test("fsync runs before append returns, and a failed fsync is a failed write", () => {
	let synced = 0;
	const log = SessionLog.open(fresh(), { io: { fsyncSync: (fd) => { synced++; fs.fsyncSync(fd); } } });
	const atOpen = synced;
	log.append(rec(1));
	assert.equal(synced, atOpen + 1);
	log.close();
	let armed = false;
	const bad = SessionLog.open(fresh(), { io: { fsyncSync: (fd) => { if (armed) throw new Error("EIO"); fs.fsyncSync(fd); } } });
	armed = true;
	assert.throws(() => bad.append(rec(1)), SessionWriteError);
	assert.throws(() => bad.append(rec(2)), SessionWriteError, "nothing more may be written after a failed sync");
	bad.close();
});

test("a change to this process's own segment is detected", () => {
	const a = SessionLog.open(fresh());
	a.append(rec(1));
	fs.appendFileSync(a.file, "x");
	assert.throws(() => a.append(rec(2)), SessionWriteError);
	a.close();
});

test("a paused writer cannot damage the new owner's history", () => {
	// The window no wait time closes: A has passed its fence check and is frozen just before writing. B takes over and
	// writes. A then wakes up and completes its write.
	const file = build(2);
	let armed = false;
	const a = SessionLog.open(file, {
		guard: () => {},
		io: {
			writeSync: (f, b, o, l) => {
				if (armed) {
					armed = false;
					const owner = SessionLog.open(file);
					owner.append(rec(50));
					owner.close();
				}
				return fs.writeSync(f, b, o, l);
			},
		},
	});
	armed = true;
	a.append(rec(40)); // Acknowledged to a process that has already been replaced.
	a.append(rec(41));
	a.close();
	const { entries } = look(file);
	assert.deepEqual(entries, [rec(0), rec(1), rec(50)], "the history must load, without the late writes");
	const next = SessionLog.open(file);
	next.append(rec(51));
	next.close();
	assert.deepEqual(look(file).entries, [rec(0), rec(1), rec(50), rec(51)]);
});

test("a new generation stops the old writer before its next write", async () => {
	const dir = path.join(ROOT, `fence${n++}`);
	const file = fresh();
	const first = await Fence.acquire(dir, 0);
	const a = SessionLog.open(file, { guard: () => first.check(), generation: first.generation });
	a.append(rec(1));
	const second = await Fence.acquire(dir, 0);
	assert.equal(second.generation, first.generation + 1);
	assert.throws(() => a.append(rec(2)), FencedError);
	assert.throws(() => a.append(rec(3)), SessionWriteError);
	a.close();
	const b = SessionLog.open(file, { guard: () => second.check(), generation: second.generation });
	b.append(rec(4));
	b.close();
	assert.deepEqual(look(file).entries, [rec(1), rec(4)]);
	assert.throws(() => SessionLog.open(file, { guard: () => first.check(), generation: first.generation }), FencedError, "a replaced process must not even start a segment");
});

test("a write that ends after this process was replaced is not acknowledged", async () => {
	const dir = path.join(ROOT, `fence${n++}`);
	const file = fresh();
	const first = await Fence.acquire(dir, 0);
	let armed = false;
	const a = SessionLog.open(file, {
		guard: () => first.check(),
		generation: first.generation,
		io: { writeSync: (f, b, o, l) => { const w = fs.writeSync(f, b, o, l); if (armed) { armed = false; Fence.claim(dir).revokeOlder(); } return w; }, fsyncSync: fs.fsyncSync },
	});
	armed = true;
	assert.throws(() => a.append(rec(1)), FencedError);
	assert.throws(() => a.append(rec(2)), SessionWriteError);
	a.close();
});

// Review 7a, #2: L takes a low number and freezes; A owns; B replaces A; L wakes up. A must stay stopped.
test("three starters: one waking up late cannot bring a replaced writer back", async () => {
	const dir = path.join(ROOT, `fence${n++}`);
	const file = fresh();
	const late = Fence.claim(dir);
	const a = await Fence.claim(dir).takeOver(0);
	const log = SessionLog.open(file, { guard: () => a.check(), generation: a.generation });
	log.append(rec(0));
	const b = await Fence.claim(dir).takeOver(0);
	await assert.rejects(late.takeOver(0), FencedError, "the late starter finds it lost");
	b.check();
	assert.throws(() => a.check(), FencedError);
	assert.throws(() => log.append(rec(1)), FencedError);
	log.close();
	const owner = SessionLog.open(file, { guard: () => b.check(), generation: b.generation });
	owner.append(rec(2));
	owner.close();
	assert.deepEqual(look(file).entries, [rec(0), rec(2)]);
});

// The only property that matters to a caller: whatever append() returned for can be read back afterwards.
for (const where of ["before the check", "before the write", "after the write", "after the sync"]) {
	test(`replaced ${where}: everything acknowledged stays readable`, async () => {
		const dir = path.join(ROOT, `fence${n++}`);
		const file = fresh();
		const first = await Fence.acquire(dir, 0);
		const acked = [];
		let armed = false;
		let owner;
		const takeOver = (at) => {
			if (!armed || at !== where) return;
			armed = false;
			const c = Fence.claim(dir);
			c.revokeOlder();
			owner = SessionLog.open(file, { generation: c.generation });
			owner.append(rec(50));
			acked.push(rec(50));
		};
		const log = SessionLog.open(file, {
			generation: first.generation,
			guard: () => { takeOver("before the check"); first.check(); },
			io: {
				writeSync: (f, b, o, l) => { takeOver("before the write"); const w = fs.writeSync(f, b, o, l); takeOver("after the write"); return w; },
				fsyncSync: (f) => { fs.fsyncSync(f); takeOver("after the sync"); },
			},
		});
		log.append(rec(0));
		acked.push(rec(0));
		armed = true;
		assert.throws(() => log.append(rec(1)), FencedError, "a replaced writer must not be told its write is safe");
		log.close();
		owner.close();
		const seen = look(file).entries;
		for (const r of acked) assert.ok(seen.some((e) => e.i === r.i), `acknowledged ${r.i} is gone: ${JSON.stringify(seen.map((e) => e.i))}`);
		assert.equal(new Set(seen.map((e) => e.i)).size, seen.length);
	});
}

test("a revocation always gets fully written, or the start fails", () => {
	const fd = fs.openSync(path.join(ROOT, `w${n++}`), "w");
	writeAll(fd, Buffer.from("revoked by 7\n"), (f, b, o) => fs.writeSync(f, b, o, 1));
	assert.throws(() => writeAll(fd, Buffer.from("x"), () => 0), /no progress/);
	fs.closeSync(fd);
});

test("a claim whose owner died before taking over does not stop later starts", async () => {
	const dir = path.join(ROOT, `fence${n++}`);
	const first = await Fence.acquire(dir, 0);
	Fence.claim(dir); // Its owner never gets further than this.
	const next = await Fence.acquire(dir, 0);
	assert.equal(next.generation, first.generation + 2);
	assert.throws(() => first.check(), FencedError);
});

// Review 8a, #6. B took a number and damaged or lost its claim before revoking A. That says nothing about A, who is still
// writing, so C must revoke A itself; otherwise A keeps acknowledging records past the length C sealed.
for (const damage of ["lost", "garbled"]) {
	test(`a ${damage} claim does not count as proof that lower ones are revoked`, async () => {
		const dir = path.join(ROOT, `fence${n++}`);
		const a = await Fence.acquire(dir, 0);
		const b = Fence.claim(dir);
		Fence.claim(dir); // Keeps the number in use: a lost top claim would simply be claimed again, which tests nothing.
		const file = path.join(dir, `generation.${b.generation}`);
		if (damage === "lost") fs.rmSync(file);
		else fs.writeFileSync(file, "garbage\n");
		await Fence.acquire(dir, 0);
		assert.throws(() => a.check(), FencedError);
	});
}

test("generation numbers are never reused, however many restarts happen", async () => {
	const dir = path.join(ROOT, `fence${n++}`);
	const seen = [];
	for (let i = 0; i < 30; i++) seen.push((await Fence.acquire(dir, 0)).generation);
	assert.deepEqual(seen, Array.from({ length: 30 }, (_, i) => i + 1));
	assert.equal(fs.readdirSync(dir).filter((f) => /^generation\.\d+$/.test(f)).length, 30, "claims are kept, so an old number cannot be claimed again");
});

test("concurrent starters in separate processes: never two owners of one number, and it always ends", async () => {
	const starter = fileURLToPath(new URL("./fence_starter.mjs", import.meta.url));
	for (let round = 0; round < 5; round++) {
		const dir = path.join(ROOT, `fence${n++}`);
		const runs = Array.from({ length: 8 }, () => new Promise((resolve) => {
			const c = spawn(process.execPath, [starter, dir], { stdio: ["ignore", "pipe", "inherit"] });
			let out = "";
			c.stdout.on("data", (d) => { out += d; });
			const kill = setTimeout(() => c.kill("SIGKILL"), 30_000);
			c.on("close", () => { clearTimeout(kill); resolve(out.trim()); });
		}));
		const outs = await Promise.all(runs);
		assert.ok(outs.every((o) => /^(owner \d+|lost)$/.test(o)), `round ${round}: a starter hung or crashed: ${outs}`);
		const owners = outs.filter((o) => o.startsWith("owner"));
		assert.equal(new Set(owners).size, owners.length, `round ${round}: one number, two owners: ${owners}`);
	}
});

test("kill -9 during appends: nothing acknowledged is ever lost and the history stays loadable", async () => {
	const file = fresh();
	const writer = fileURLToPath(new URL("./killwriter.mjs", import.meta.url));
	let total = 0;
	const rounds = Number(process.env.KILL_ROUNDS ?? 30);
	const acked = new Set();
	for (let round = 0; round < rounds; round++) {
		const child = spawn(process.execPath, [writer, file], { stdio: ["ignore", "pipe", "inherit"] });
		let out = "";
		child.stdout.on("data", (d) => { out += d; });
		await new Promise((r) => setTimeout(r, 40 + Math.random() * 80));
		child.kill("SIGKILL");
		await new Promise((r) => child.on("close", r));
		for (const l of out.split("\n")) if (/^\d+$/.test(l)) acked.add(Number(l));
		const ids = new Set(look(file).entries.map((e) => e.i));
		for (const id of acked) assert.ok(ids.has(id), `round ${round}: acknowledged ${id} lost`);
		assert.equal(ids.size, look(file).entries.length, `round ${round}: duplicates`);
		total = ids.size;
	}
	assert.ok(total > 0, "the writer never got a record out, so this test proved nothing");
});

test("a tool call left unanswered gets an 'outcome unknown' result; nothing re-runs", () => {
	const user = { role: "user", content: [] };
	const call = { role: "assistant", content: [{ type: "toolCall", id: "a", name: "send" }, { type: "toolCall", id: "b", name: "send" }] };
	const done = { role: "toolResult", toolCallId: "a", toolName: "send", content: [], isError: false };
	const { messages, added } = repairTranscript([user, call, done]);
	assert.deepEqual(messages.map((m) => m.toolCallId ?? m.role), ["user", "assistant", "a", "b"]);
	assert.equal(added.length, 1);
	assert.equal(added[0].isError, true);
	assert.match(added[0].content[0].text, /结果未知/);
	assert.equal(repairTranscript(messages).added.length, 0, "repair must be idempotent");
});

test("a real result on record beats 'unknown'", () => {
	const call = { role: "assistant", content: [{ type: "toolCall", id: "a", name: "send" }] };
	const log = [{ t: "op_start", opId: "o1", tool: "send", toolCallId: "a" }, { t: "op_end", opId: "o1", tool: "send", toolCallId: "a", detached: false, isError: false, text: "created:id42" }];
	const { added } = repairTranscript([call], knownResults(log));
	assert.equal(added[0].content[0].text, "created:id42");
	assert.equal(added[0].isError, false);
	const { notes } = reconcileOps(log, new Set());
	assert.equal(notes.length, 0, "already delivered through the repaired tool result");
});

test("what is owed to the model survives repeated crashes until the message carrying it is durable", () => {
	const log = [{ t: "op_start", opId: "o1", tool: "x", toolCallId: "c1" }, { t: "steer_recv", id: "s1", text: "wait" }];
	for (let crash = 0; crash < 3; crash++) {
		const { notes, records } = reconcileOps(log, new Set());
		assert.deepEqual(notes.map((m) => m.zgLateOp ?? m.zgSteerId), ["o1", "s1"], `crash ${crash}`);
		assert.deepEqual(records, [], "nothing may be marked delivered yet");
	}
	assert.deepEqual(reconcileOps([...log, { t: "op_told", opId: "o1" }, { t: "steer_done", id: "s1" }], new Set()).notes, []);
	const late = [{ t: "op_start", opId: "o2", tool: "x", toolCallId: "c2" }, { t: "op_end", opId: "o2", tool: "x", toolCallId: "c2", detached: true, text: "done" }];
	assert.match(reconcileOps(late, new Set()).notes[0].content[0].text, /done/);
	assert.equal(reconcileOps(log.slice(1), new Set(), [{ role: "user", zgSteerId: "s1" }]).notes.length, 0, "already in the transcript");
});

test("a result already in the transcript is not delivered twice after a crash", () => {
	// The message carrying the result is durable; the crash came before the record saying so.
	const late = { role: "user", content: [{ type: "text", text: "late" }], zgLateOp: "op-1" };
	const entries = [{ t: "op_start", opId: "op-1", tool: "t", toolCallId: "c1" }, { t: "op_end", opId: "op-1", detached: true, text: "x", toolCallId: "c1" }, { t: "msg", m: late }];
	const { notes, records } = reconcileOps(entries, new Set(), [late]);
	assert.equal(notes.length, 0);
	assert.deepEqual(records, [{ t: "op_told", opId: "op-1" }]);
});

test("a dropped message and a compaction replay the way they happened, in either order", () => {
	const [a, b, bad] = [{ i: "a" }, { i: "b" }, { i: "bad" }];
	const before = [{ t: "msg", m: a }, { t: "msg", m: bad }, { t: "drop_last" }, { t: "compact", messages: [a, b] }];
	assert.deepEqual(replayEntries(before).messages, [a, b]);
	const after = [{ t: "compact", messages: [a, b] }, { t: "msg", m: bad }, { t: "drop_last" }];
	assert.deepEqual(replayEntries(after).messages, [a, b]);
});

test("a cancelled queued op keeps its error mark across a restart, and a recovered steer can still be reported delivered", () => {
	const entries = [
		{ t: "op_start", opId: "o", tool: "t", toolCallId: "c" },
		{ t: "op_end", opId: "o", notSent: true, detached: true, isError: true, text: "没有执行", toolCallId: "c" },
		{ t: "steer_recv", id: "s", text: "改成 B" },
	];
	const { notes } = reconcileOps(entries, new Set(), []);
	assert.match(notes[0].content[0].text, /并报错/);
	assert.equal(notes[1].zgSteer, "改成 B");
	assert.equal(notes[1].zgSteerId, "s");
});

test("a compact record replaces history; a tear inside it keeps the old history", () => {
	const file = fresh();
	const log = SessionLog.open(file);
	log.append({ t: "hdr", cwd: "/w" });
	log.append({ t: "msg", m: { role: "user", i: 1 } });
	log.append({ t: "compact", messages: [{ role: "user", i: 2 }] });
	log.close();
	assert.deepEqual(replayEntries(look(file).entries), { cwd: "/w", messages: [{ role: "user", i: 2 }] });
	const seg = lastSeg(file);
	const whole = fs.readFileSync(seg);
	fs.writeFileSync(seg, whole.subarray(0, whole.length - 3));
	assert.deepEqual(replayEntries(look(file).entries).messages, [{ role: "user", i: 1 }]);
});

test("a dropped empty answer does not come back after a restart", () => {
	assert.deepEqual(
		replayEntries([{ t: "msg", m: { role: "user" } }, { t: "msg", m: { role: "assistant", empty: true } }, { t: "drop_last" }, { t: "msg", m: { role: "assistant" } }]).messages,
		[{ role: "user" }, { role: "assistant" }],
	);
});
