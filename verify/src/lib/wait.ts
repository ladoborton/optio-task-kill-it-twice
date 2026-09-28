export const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/** Polls `probe` until it returns a non-undefined value, or fails with `what` after `timeoutMs`. */
export async function waitFor<T>(
  what: string,
  probe: () => Promise<T | undefined>,
  timeoutMs: number,
  intervalMs = 250,
): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const value = await probe();
    if (value !== undefined) return value;
    await sleep(intervalMs);
  }
  throw new Error(`timed out after ${Math.round(timeoutMs / 1000)}s waiting for ${what}`);
}
