import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { dataSourceOptions } from '../../database/data-source';
import { CustomerEventsConsumer } from './customer-events.consumer';
import { ProjectionRepository } from './projection.repository';

@Module({
  imports: [TypeOrmModule.forRoot(dataSourceOptions)],
  providers: [ProjectionRepository, CustomerEventsConsumer],
})
export class ConsumerModule {}
