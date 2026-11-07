// Op registry, with no model and no server: what polling, cancelling, timing out and a failing log promise.
import assert from "node:assert/strict";
import { after, test } from "node:test";
import { OpRegistry, placeLiveNote, SerialQueue } from "../dist/ops.js";

// The timers are unref'd because a real process stays alive on stdin; a test needs something else to hold the loop.
const keepAlive = setInterval(() => {}, 1000);
after(() => clearInterval(keepAlive));

function setup(waitMs, persist) {
	const log = [];
	const late = [];
	const ops = new OpRegistry("op", { persist: persist ?? ((e) => log.push(e)), onLate: (l) => late.push(l), steerPending: () => false }, waitMs);
	return { ops, log, late, queue: new SerialQueue() };
}
function gate() {
	let open;
	const p = new Promise((r) => { open = r; });
	return { p, open };
}
const call = (queue, tool, g, sends, args) => ({
	server: "s", tool, args: args ?? { tool }, toolCallId: `c-${tool}`,
	send: () => queue.enqueue(async () => {
		sends.push(tool);
		await g.p;
		return { content: [{ type: "text", text: `done:${tool}` }], details: undefined };
	}),
});
const tick = (ms = 5) => new Promise((r) => setTimeout(r, ms));
const textOf = (r) => r.content[0].text;
// A call that really went out waits on its gate for ever. Without a limit the test would hang and the runner would just
// stop, which is not a failure, so a broken duplicate check would go unnoticed.
const orSent = (p) => Promise.race([p, tick(100).then(() => ({ content: [{ type: "text", text: "(really sent)" }] }))]);

test("polling an op tells the truth at each stage", async () => {
	const { ops, queue, late } = setup();
	const g = gate();
	const sends = [];
	const running = ops.run(call(queue, "a", g, sends));
	await tick();
	assert.match(ops.poll("op-1", "poll"), /还在执行/);
	ops.yieldAll();
	assert.match(textOf(await running), /已转入后台/);
	assert.match(ops.liveNote(), /op-1/);
	assert.equal(late.length, 0);
	g.open();
	await tick();
	assert.equal(late.length, 1);
	assert.equal(ops.liveNote(), undefined);
	assert.match(ops.poll("op-1", "poll"), /已结束[\s\S]*done:a/);
	assert.equal(ops.poll("nope", "poll"), undefined);
});

test("kill drops a queued op, ends its call, and only says so when it really never ran", async () => {
	const { ops, queue, log } = setup();
	const first = gate();
	const sends = [];
	void ops.run(call(queue, "a", first, sends));
	const queued = ops.run(call(queue, "b", gate(), sends));
	await tick();
	assert.match(ops.poll("op-2", "poll"), /还在排队/);
	assert.match(ops.poll("op-1", "kill"), /无法撤回/);
	assert.match(ops.poll("op-2", "kill"), /不会执行/);
	assert.match(textOf(await queued), /没有执行/, "the tool call waiting on it must end too");
	first.open();
	await tick();
	assert.deepEqual(sends, ["a"]);
	assert.ok(log.some((e) => e.t === "op_end" && e.opId === "op-2" && e.notSent));
});

test("a cancel still drops a queued op that had already yielded", async () => {
	const { ops, queue, late } = setup();
	const first = gate();
	const sends = [];
	void ops.run(call(queue, "a", first, sends));
	const ctl = new AbortController();
	const queued = ops.run(call(queue, "b", gate(), sends), ctl.signal);
	await tick();
	ops.yieldAll();
	assert.match(textOf(await queued), /还在排队/);
	ctl.abort();
	first.open();
	await tick(10);
	assert.deepEqual(sends, ["a"], "a cancelled op must not be sent later");
	assert.ok(late.some((l) => l.tool === "b" && l.isError), "the model hears it did not run");
});

test("a timeout ends the wait, not the operation", async () => {
	const { ops, queue, late } = setup(20);
	const g = gate();
	const sends = [];
	const args = { same: 1 };
	const first = ops.run(call(queue, "a", g, sends, args));
	assert.match(textOf(await first), /等了[\s\S]*转入后台/);
	assert.equal(ops.inFlight(), 1, "the server is still working on it");
	const again = await ops.run(call(queue, "a", gate(), sends, args));
	assert.match(textOf(again), /已拒绝/);
	const other = gate();
	void ops.run(call(queue, "b", other, sends));
	await tick();
	assert.deepEqual(sends, ["a"], "the queue must stay held: the server has not finished");
	g.open();
	other.open();
	await tick(10);
	assert.deepEqual(sends, ["a", "b"]);
	assert.ok(late.some((l) => l.tool === "a" && /done:a/.test(l.text)));
});

test("the same arguments in another key order are still the same operation", async () => {
	const { ops, queue } = setup();
	const g = gate();
	const sends = [];
	const first = ops.run(call(queue, "a", g, sends, { x: 1, y: { p: 1, q: [2, { r: 3, s: 4 }] } }));
	await tick();
	ops.yieldAll();
	await first;
	const again = await orSent(ops.run(call(queue, "a", gate(), sends, { y: { q: [2, { s: 4, r: 3 }], p: 1 }, x: 1 })));
	assert.match(textOf(again), /已拒绝/);
	const other = ops.run(call(queue, "a", gate(), sends, { x: 1, y: { p: 1, q: [{ r: 3, s: 4 }, 2] } }));
	g.open();
	await tick(10);
	assert.deepEqual(sends, ["a", "a"], "array order does matter: a different call goes through");
	void other;
});

test("a finished op the model has not heard about is not sent again, and its real result comes back", async () => {
	const { ops, queue, late } = setup();
	const g = gate();
	const sends = [];
	const args = { same: 1 };
	const first = ops.run(call(queue, "a", g, sends, args));
	await tick();
	ops.yieldAll();
	await first;
	g.open();
	await tick();
	assert.equal(late.length, 1);
	const again = await orSent(ops.run(call(queue, "a", gate(), sends, args)));
	assert.match(textOf(again), /没有再次发送[\s\S]*done:a/);
	assert.deepEqual(sends, ["a"]);
	ops.markTold(late[0].opId);
	const g2 = gate();
	const deliberate = ops.run(call(queue, "a", g2, sends, args));
	g2.open();
	await deliberate;
	assert.deepEqual(sends, ["a", "a"], "once told, asking again is the model's decision");
});

test("an op that was dropped before sending can be retried: nothing ran", async () => {
	const { ops, queue } = setup();
	const first = gate();
	const sends = [];
	const args = { same: 1 };
	void ops.run(call(queue, "a", first, sends));
	const queued = ops.run(call(queue, "b", gate(), sends, args));
	await tick();
	ops.yieldAll();
	await queued;
	ops.dropAllQueued();
	first.open();
	await tick(10);
	const g = gate();
	const retry = ops.run(call(queue, "b", g, sends, args));
	g.open();
	await retry;
	assert.deepEqual(sends, ["a", "b"]);
});

test("a cancel after the op's own turn ended still drops it", async () => {
	const { ops, queue, late } = setup();
	const first = gate();
	const sends = [];
	void ops.run(call(queue, "a", first, sends));
	const queued = ops.run(call(queue, "b", gate(), sends)); // No signal that a later cancel could reach.
	await tick();
	ops.yieldAll();
	await queued;
	ops.dropAllQueued();
	assert.ok(late.some((l) => l.tool === "b" && l.isError));
	first.open();
	await tick(10);
	assert.deepEqual(sends, ["a"]);
});

test("old operation ids still answer after a restart", () => {
	const { ops } = setup();
	ops.restoreFinished([
		{ t: "op_start", opId: "old-1", tool: "a", toolCallId: "c1" },
		{ t: "op_end", opId: "old-1", detached: true, isError: false, text: "done:a" },
		{ t: "op_start", opId: "old-2", tool: "b", toolCallId: "c2" },
	]);
	assert.match(ops.poll("old-1", "poll"), /已结束[\s\S]*done:a/);
	assert.match(ops.poll("old-2", "poll"), /结果未知/);
	assert.equal(ops.poll("old-3", "poll"), undefined);
});

test("a long result says it was cut", async () => {
	const { ops, queue } = setup();
	const g = gate();
	const longCall = { server: "s", tool: "t", args: {}, toolCallId: "c", send: () => queue.enqueue(async () => { await g.p; return { content: [{ type: "text", text: "x".repeat(9000) }], details: undefined }; }) };
	const running = ops.run(longCall);
	await tick();
	ops.yieldAll();
	await running;
	g.open();
	await tick();
	assert.match(ops.poll("op-1", "poll"), /结果过长[\s\S]*共 9000 个字符/);
});

test("the background note is never the newest message, and never splits a call from its result", () => {
	const user = { role: "user", content: "go" };
	const call = { role: "assistant", content: [{ type: "toolCall", id: "1" }] };
	const result = { role: "toolResult", toolCallId: "1" };
	const note = { role: "user", zgLiveNote: true };
	assert.deepEqual(placeLiveNote([user, call, result], note), [note, user, call, result]);
	assert.deepEqual(placeLiveNote([call, result], note), [note, call, result]);
	const later = { role: "user", content: "steer" };
	assert.deepEqual(placeLiveNote([user, call, result, later], note), [user, call, result, note, later]);
});

test("nothing is sent when the log cannot record it", async () => {
	const { ops, queue } = setup(undefined, () => { throw new Error("EIO"); });
	const sends = [];
	const r = await ops.run(call(queue, "a", gate(), sends));
	assert.match(textOf(r), /没有执行/);
	await tick();
	assert.deepEqual(sends, []);
	assert.equal(ops.inFlight(), 0);
});
