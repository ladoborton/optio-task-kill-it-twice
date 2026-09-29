import { Provider } from '@nestjs/common';
import { CircuitBreaker } from '../../shared/circuit-breaker';
import { config } from '../../shared/config';
import { log } from '../../shared/logger';

// One breaker per sink, shared by every loop that writes to it (SPEC §7.2).
export const ES_BREAKER = Symbol('ES_BREAKER');
export const STREAM_BREAKER = Symbol('STREAM_BREAKER');

const EVENT = { open: 'circuit.opened', half_open: 'circuit.half_open', closed: 'circuit.closed' } as const;

const breaker = (sink: string) =>
  new CircuitBreaker(sink, config.breakerThreshold, config.breakerOpenMs, (from, to, failures) =>
    log(EVENT[to], { sink, from, failures }, to === 'open' ? 'warn' : 'info'),
  );

export const breakerProviders: Provider[] = [
  { provide: ES_BREAKER, useFactory: () => breaker('elasticsearch') },
  { provide: STREAM_BREAKER, useFactory: () => breaker('rabbitmq') },
];
