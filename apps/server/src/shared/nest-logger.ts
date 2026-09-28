import { LoggerService } from '@nestjs/common';
import { log } from './logger';

// Routes Nest's own messages through the JSON logger, so `docker compose logs` stays one format.
export class NestJsonLogger implements LoggerService {
  log(message: unknown, ...rest: unknown[]): void {
    log('nest', { message: String(message), context: rest.at(-1) });
  }
  warn(message: unknown, ...rest: unknown[]): void {
    log('nest', { message: String(message), context: rest.at(-1) }, 'warn');
  }
  error(message: unknown, ...rest: unknown[]): void {
    log('nest', { message: String(message), details: rest }, 'error');
  }
  debug(): void {}
  verbose(): void {}
}
