import { NestFactory } from '@nestjs/core';
import { NestJsonLogger } from '../../shared/nest-logger';
import { ConsumerModule } from './consumer.module';

// ROLE=consumer: a separate process with its own failure domain (SPEC §5.1).
export async function runConsumer(): Promise<void> {
  const app = await NestFactory.createApplicationContext(ConsumerModule, { logger: new NestJsonLogger() });
  app.enableShutdownHooks();
}
