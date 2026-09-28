import { NestFactory } from '@nestjs/core';
import { NestJsonLogger } from '../../shared/nest-logger';
import { PipelineModule } from './pipeline.module';

// ROLE=pipeline. No HTTP yet (the /metrics endpoint arrives with G5); an application context is
// enough to run the loops with dependency injection and lifecycle hooks.
export async function runPipeline(): Promise<void> {
  const app = await NestFactory.createApplicationContext(PipelineModule, { logger: new NestJsonLogger() });
  // SIGTERM (docker stop / compose down) → beforeApplicationShutdown → loops finish their step.
  app.enableShutdownHooks();
}
