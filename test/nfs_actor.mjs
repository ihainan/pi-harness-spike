// One process in a storage scenario. Takes JSON commands on stdin and answers on stdout, so nothing about coordinating a
// test depends on the filesystem under test. At a named point it freezes itself with SIGSTOP; the driver decides when it
// wakes up. A frozen process is the real thing: it holds its descriptors and its view, and does nothing at all.
import * as fs from "node:fs";
import * as path from "node:path";
import * as readline from "node:readline";
import { Fence } from "../dist/fence.js";
import { SessionLog } from "../dist/sessionlog.js";

const say = (o) => fs.writeSync(1, `${JSON.stringify(o)}\n`);
let claim, fence, log;
let pauseAt = null;

function point(name) {
	if (pauseAt !== name) return;
	pauseAt = null;
	say({ at: name });
	process.kill(process.pid, "SIGSTOP");
}

const logOptions = () => ({
	generation: fence.generation,
	guard: () => { point("before-check"); fence.check(); },
	io: {
		writeSync: (f, b, o, l) => { point("before-write"); const w = fs.writeSync(f, b, o, l); point("after-write"); return w; },
		fsyncSync: (f) => { fs.fsyncSync(f); point("after-sync"); },
	},
});

const COMMANDS = {
	claim: ({ dir }) => { claim = Fence.claim(dir); return { generation: claim.generation }; },
	revoke: () => { claim.revokeOlder(); return {}; },
	takeover: async ({ grace }) => { fence = await claim.takeOver(grace ?? 0); return { generation: fence.generation }; },
	open: ({ file }) => { log = SessionLog.open(file, logOptions()); return { entries: log.entries }; },
	append: ({ entry }) => { log.append(entry); return { acked: entry }; }, // The only line that counts as an acknowledgement.
	close: () => { log.close(); return {}; },
	read: ({ file }) => ({ entries: SessionLog.read(file).entries }),
	readSegment: ({ file }) => { const b = fs.readFileSync(file); return { bytes: b.length }; },
	probe: ({ file }) => ({ exists: fs.existsSync(file) }), // Warms this client's cache, including "does not exist".
	pause: ({ at }) => { pauseAt = at; return {}; },
	exit: () => process.exit(0),
};

say({ pid: process.pid, cwd: path.basename(process.cwd()) });
for await (const line of readline.createInterface({ input: process.stdin })) {
	if (!line.trim()) continue;
	const { id, cmd, ...args } = JSON.parse(line);
	try {
		say({ id, ok: true, ...(await COMMANDS[cmd](args)) });
	} catch (err) {
		say({ id, ok: false, name: err?.name, error: String(err?.message ?? err) });
	}
}
