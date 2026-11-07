import { Fence, FencedError } from "../dist/fence.js";
try {
	const f = await Fence.acquire(process.argv[2], 0);
	f.check();
	console.log(`owner ${f.generation}`);
} catch (e) {
	if (!(e instanceof FencedError)) throw e;
	console.log("lost");
}
