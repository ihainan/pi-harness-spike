// Breaks the session log on purpose and confirms the named test goes red. A check that has never failed proves nothing.
// Exit: 0 every fault detected; 1 a fault went undetected; 2 the run itself is not evidence (baseline red or tests did not run).
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const tests = ["./sessionlog.test.mjs", "./ops.test.mjs"].map((f) => fileURLToPath(new URL(f, import.meta.url)));
const CASES = [
	["log_skip_corrupt", "any single bit flipped"],
	["log_no_crc", "any single bit flipped"],
	["log_no_seq", "duplicated and missing"],
	["log_no_post_guard", "a write that ends after this process was replaced"],
	["log_no_fsync", "fsync runs before append"],
	["log_no_size_check", "a change to this process's own segment is detected"],
	["log_no_seal", "a paused writer cannot damage the new owner's history"],
	["op_no_known_result", "a real result on record beats 'unknown'"],
	["log_no_fence", "a new generation stops the old writer"],
	["fence_no_revoke", "three starters"],
	["fence_trust_damaged", "a lost claim does not count"],
	["log_latest_opener", "a late opener cannot cut"],
	["log_no_tool_repair", "a tool call left unanswered"],
	["op_no_restore", "old operation ids still answer"],
	["live_note_last", "the background note is never the newest message"],
	["op_key_as_written", "the same arguments in another key order"],
	["op_no_finished_dedupe", "a finished op the model has not heard about"],
	["op_no_transcript_told", "a result already in the transcript"],
];
function run(fault) {
	const r = spawnSync(process.execPath, ["--test", "--test-reporter=tap", ...tests], { encoding: "utf8", env: { ...process.env, PI_FAULT: fault, KILL_ROUNDS: "3" }, timeout: 600_000 });
	const lines = r.stdout.split("\n");
	const failed = lines.filter((l) => /^\s*not ok \d+ - /.test(l)).map((l) => l.replace(/^\s*not ok \d+ - /, ""));
	const passed = lines.filter((l) => /^\s*ok \d+ - /.test(l)).length;
	return { status: r.status, failed, ran: failed.length + passed };
}
const base = run("");
console.log(JSON.stringify({ fault: "(none)", ...base }));
if (base.status !== 0 || base.ran === 0) process.exit(2);
let missed = 0;
let infra = 0;
for (const [fault, expected] of CASES) {
	const r = run(fault);
	const detected = r.failed.some((n) => n.startsWith(expected));
	if (r.ran === 0) infra++;
	else if (!detected) missed++;
	console.log(JSON.stringify({ fault, expected, detected, ran: r.ran, failed: r.failed }));
}
console.log(`detected ${CASES.length - missed - infra} of ${CASES.length}, infra errors ${infra}`);
process.exit(infra ? 2 : missed ? 1 : 0);
