/**
 * SPEC §7.2: exponential backoff with jitter. `attempt` is 1 for the first retry.
 *
 * The ceiling doubles per attempt (base, 2·base, 4·base … max). The delay is drawn from
 * [ceiling/2, ceiling]: never zero (no busy loop), yet spread out so that several retrying
 * processes don't hit a recovering sink at the same instant.
 */
export function backoffDelay(attempt: number, baseMs: number, maxMs: number, random: () => number = Math.random): number {
  const ceiling = Math.min(maxMs, baseMs * 2 ** Math.max(0, attempt - 1));
  return Math.round(ceiling / 2 + (random() * ceiling) / 2);
}
