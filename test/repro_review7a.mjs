// Reproduces the two protocol-level counterexamples from review 7a against the build under test. Exit 0 = both reproduced.
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { Fence } from "../dist/fence.js";
import { SessionLog } from "../dist/sessionlog.js";

const root = fs.mkdtempSync(path.join(process.env.LOG_DIR ?? os.tmpdir(), "repro7a-"));
const dirA = path.join(root, "a");
fs.mkdirSync(dirA);

// #1: a late opener writes head back. A has its segment and is frozen before publishing; B takes over and acknowledges R.
const f1 = path.join(root, "s1.jsonl");
const seed = SessionLog.open(f1, { generation: 1 });
seed.append({ i: "seed" });
seed.close();
const a = SessionLog.open(f1, { generation: 2 });
a.close();
const b = SessionLog.open(f1, { generation: 3 });
b.append({ i: "R" });
b.close();
fs.writeFileSync(`${f1}.head`, `${String(2).padStart(20, "0")}\n`); // What A's frozen write does when it wakes up.
const seen = SessionLog.read(f1).entries.map((e) => e.i);
const lostR = !seen.includes("R");
console.log(`#1 acknowledged R ${lostR ? "LOST" : "kept"}: ${JSON.stringify(seen)}`);

// #2: a late starter writes the generation back, and a replaced writer comes back to life.
const fdir = path.join(root, "fence");
const fA = await Fence.acquire(fdir, 0); // Stands in for L's number being lower: L is simulated below.
const fB = await Fence.acquire(fdir, 0);
const fC = await Fence.acquire(fdir, 0);
let revived = false;
if (fs.existsSync(path.join(fdir, "generation"))) {
	fs.writeFileSync(path.join(fdir, "generation"), `${String(fA.generation).padStart(20, "0")}\n`); // L wakes up.
	try {
		fB.check();
		revived = true;
	} catch {}
}
console.log(`#2 replaced writer ${revived ? "REVIVED" : "stays stopped"} (current owner ${fC.generation})`);
fs.rmSync(root, { recursive: true, force: true });
process.exit(lostR && revived ? 0 : 1);
