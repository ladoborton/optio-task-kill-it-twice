export type CircuitState = 'closed' | 'open' | 'half_open';

/** The sink is known to be down; the call was not made. Retry after `retryAfterMs`. */
export class CircuitOpenError extends Error {
  constructor(readonly sink: string, readonly retryAfterMs: number) {
    super(`${sink} circuit open, next probe in ${retryAfterMs} ms`);
  }
}

/** While one probe is in flight, other callers wait this long before asking again. */
const PROBE_BUSY_MS = 1_000;

/**
 * SPEC §7.2: one breaker per sink, shared by every loop that writes to it (pure, unit-tested).
 *
 * closed    — calls go through; `threshold` consecutive failures open the circuit.
 * open      — calls fail immediately with CircuitOpenError, the sink is not touched, until
 *             `openMs` has passed.
 * half_open — exactly one call (the probe) goes through; success closes the circuit, failure
 *             opens it for another `openMs`.
 *
 * Why on top of per-loop backoff: backoff alone lets each loop's wait grow to its cap, so after a
 * sink comes back nobody may try again for up to that long (30 s with the v1 cap; G3 measured the
 * first write 3.8 s and 8.5 s after recovery — luck of where in the window the sink returned).
 * With the breaker, the sink sees one probe per `openMs` however many loops wait on it, and the
 * worst case is one probe interval (G3: 3.1–3.7 s).
 */
export class CircuitBreaker {
  private state: CircuitState = 'closed';
  private failures = 0;
  private openedAt = 0;
  private probing = false;

  constructor(
    readonly sink: string,
    private readonly threshold: number,
    private readonly openMs: number,
    private readonly onTransition: (from: CircuitState, to: CircuitState, failures: number) => void = () => undefined,
    private readonly now: () => number = Date.now,
  ) {}

  get current(): CircuitState {
    return this.state;
  }

  async run<T>(call: () => Promise<T>): Promise<T> {
    this.admit();
    try {
      const result = await call();
      this.succeeded();
      return result;
    } catch (e) {
      this.failed();
      throw e;
    }
  }

  private admit(): void {
    if (this.state === 'open') {
      const waited = this.now() - this.openedAt;
      if (waited < this.openMs) throw new CircuitOpenError(this.sink, this.openMs - waited);
      this.transition('half_open');
    }
    if (this.state === 'half_open') {
      if (this.probing) throw new CircuitOpenError(this.sink, PROBE_BUSY_MS);
      this.probing = true;
    }
  }

  private succeeded(): void {
    this.probing = false;
    this.failures = 0;
    if (this.state !== 'closed') this.transition('closed');
  }

  private failed(): void {
    this.probing = false;
    this.failures++;
    if (this.state === 'half_open' || this.failures >= this.threshold) {
      this.openedAt = this.now();
      if (this.state !== 'open') this.transition('open');
    }
  }

  private transition(to: CircuitState): void {
    const from = this.state;
    this.state = to;
    this.onTransition(from, to, this.failures);
  }
}
