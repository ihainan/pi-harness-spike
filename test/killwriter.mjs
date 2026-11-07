// Appends as fast as it can and prints each id only after append() returned; the parent kills it with SIGKILL.
import { SessionLog } from "../dist/sessionlog.js";

const log = SessionLog.open(process.argv[2]);
let i = log.entries.length ? Math.max(...log.entries.map((e) => e.i)) + 1 : 0;
for (;;) {
	log.append({ t: "msg", i, text: "第".repeat(2000) });
	await new Promise((r) => process.stdout.write(`${i}\n`, r));
	i++;
}
