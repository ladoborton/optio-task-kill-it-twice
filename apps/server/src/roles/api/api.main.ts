import { NestFactory } from '@nestjs/core';
import { config } from '../../shared/config';
import { log } from '../../shared/logger';
import { NestJsonLogger } from '../../shared/nest-logger';
import { ApiModule } from './api.module';

// ROLE=api: HTTP for the UI and verify. A separate process from the pipeline (SPEC §5.1), so it
// keeps answering — and reporting "down" — while the pipeline is killed.
export async function runApi(): Promise<void> {
  const app = await NestFactory.create(ApiModule, { logger: new NestJsonLogger() });
  app.enableCors(); // the UI is served from its own origin
  app.enableShutdownHooks();
  await app.listen(config.apiPort);
  log('api.listening', { port: config.apiPort });
}
