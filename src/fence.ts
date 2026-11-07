// One writer per state directory, enforced with revocable claims rather than a lock.
//
// Why: an evicted pod can still be running while its replacement starts, because a ReplicaSet does not count a
// Terminating pod. On real NFS two writers corrupted or silently overwrote one log file in 12 of 12 rounds.
//
// This fence only makes a replaced process stop soon. It is not what protects the data: a process paused between its
// check and its write can still write, for as long as it stays paused. Each generation therefore writes its own log
// segment (sessionlog.ts), so that late write lands in a file nobody reads past the point the new owner recorded.
//
// How: a process claims a number with link(), which is atomic on NFS, then appends a revocation to the claim file of every
// lower number, then waits. Each owner re-reads only its own claim before and after every write; if the file no longer
// holds what it wrote, it has been replaced.
//
// The state only ever moves one way. An earlier version kept one shared "current generation" file, and a starter that
// woke up late could write a lower number over it and bring a replaced writer back to life; no check-then-write closes
// that window. Here a late starter can only revoke numbers below its own, and nothing ever un-revokes a claim.
//
// Claim numbers are dense: a starter takes the first free number above any existing one, moving up on EEXIST, which the
// server answers authoritatively even when a directory listing is stale. Numbers are never reused, claims never deleted.
//
// The claim is appended to in place. Tested on real NFS 4.1 with the earlier in-place file: the write changes the file's
// own attributes, which open() must revalidate (close-to-open), and recalls any read delegation another client holds.
import { randomUUID } from "node:crypto";
import * as fs from "node:fs";
import * as path from "node:path";
import { fault } from "./fault.js";

export class FencedError extends Error {
	constructor(message: string) {
		super(message);
		this.name = "FencedError";
	}
}

const CLAIM_TEXT = "claim\n";

/** write() may stop early, and on NFS it may do so without an error. A revocation only partly written would not count. */
export function writeAll(fd: number, buf: Buffer, write: (fd: number, buf: Buffer, off: number, len: number) => number = fs.writeSync): void {
	let off = 0;
	while (off < buf.length) {
		const n = write(fd, buf, off, buf.length - off);
		if (n <= 0) throw new Error(`write made no progress at byte ${off} of ${buf.length}`);
		off += n;
	}
}

function claimPath(dir: string, n: number): string {
	return path.join(dir, `generation.${n}`);
}

/** Anything but the untouched claim stops its owner: an unreadable or damaged claim must not excuse it. */
function isLive(file: string): boolean {
	try {
		return fs.readFileSync(file, "utf8") === CLAIM_TEXT;
	} catch (err: any) {
		if (err?.code === "ENOENT") return false;
		throw err;
	}
}

/** Only a real mark says lower numbers are done with. A missing or damaged claim says nothing about them. */
function isRevoked(file: string): boolean {
	try {
		return fs.readFileSync(file, "utf8").startsWith(`${CLAIM_TEXT}revoked by `);
	} catch (err: any) {
		if (err?.code === "ENOENT") return false;
		throw err;
	}
}

/** A number taken but not yet owned. Separate from taking over so that tests can stop a starter between the two. */
export class Claim {
	constructor(
		private readonly dir: string,
		readonly generation: number,
	) {}

	/**
	 * Revokes every lower claim. Lowest first, so "n is revoked" always implies everything below it is too: a starter that
	 * crashes halfway leaves that true, and the next one can stop walking at the first revoked claim it meets.
	 */
	revokeOlder(): void {
		if (fault("fence_no_revoke")) return;
		let low = this.generation - 1;
		while (low > 0 && !(!fault("fence_trust_damaged") ? isRevoked(claimPath(this.dir, low)) : !isLive(claimPath(this.dir, low)))) low--;
		const mark = Buffer.from(`revoked by ${this.generation}\n`, "latin1");
		for (let n = low + 1; n < this.generation; n++) {
			let fd: number;
			try {
				fd = fs.openSync(claimPath(this.dir, n), fs.constants.O_WRONLY | fs.constants.O_APPEND);
			} catch (err: any) {
				if (err?.code === "ENOENT") continue; // Nobody can be checking a claim that is not there.
				throw err;
			}
			try {
				writeAll(fd, mark);
				fs.fsyncSync(fd);
			} finally {
				fs.closeSync(fd);
			}
		}
	}

	/** Resolves once the previous owner can no longer be mid-append. Throws if a newer starter got there first. */
	async takeOver(graceMs = Number(process.env.PI_FENCE_GRACE_MS ?? 3000)): Promise<Fence> {
		this.revokeOlder();
		// A previous process existed only if a lower number exists, so a first start does not wait.
		if (this.generation > 1 && graceMs > 0) await new Promise((r) => setTimeout(r, graceMs));
		const fence = new Fence(claimPath(this.dir, this.generation), this.generation);
		fence.check();
		return fence;
	}
}

export class Fence {
	constructor(
		private readonly file: string,
		readonly generation: number,
	) {}

	/** Takes a number. The listing is only a starting point: if it is stale, EEXIST moves the claim up to the first free one. */
	static claim(dir: string): Claim {
		fs.mkdirSync(dir, { recursive: true });
		let next = 1;
		for (const name of fs.readdirSync(dir)) {
			const m = /^generation\.(\d+)$/.exec(name);
			if (m) next = Math.max(next, Number(m[1]) + 1);
		}
		const tmp = path.join(dir, `.generation.${randomUUID()}`);
		fs.writeFileSync(tmp, CLAIM_TEXT);
		for (;;) {
			try {
				fs.linkSync(tmp, claimPath(dir, next));
				break;
			} catch (err: any) {
				if (err?.code !== "EEXIST") throw err;
				next++;
			}
		}
		fs.rmSync(tmp, { force: true });
		return new Claim(dir, next);
	}

	/** Takes over the directory. Resolves once the previous owner can no longer be mid-append. */
	static async acquire(dir: string, graceMs?: number): Promise<Fence> {
		return Fence.claim(dir).takeOver(graceMs);
	}

	/** Throws once a newer process owns the directory. Call before and after every write. */
	check(): void {
		if (fault("log_no_fence")) return;
		if (!isLive(this.file)) throw new FencedError(`this process (generation ${this.generation}) was replaced`);
	}
}
