import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { dataSourceOptions } from '../../database/data-source';
import { esClientProvider } from '../../shared/elasticsearch';
import { BackfillLoop } from './backfill/backfill.loop';
import { CheckpointRepository } from './checkpoint/checkpoint.repository';
import { ControlRepository } from './control/control.repository';
import { CustomerIndex } from './es/customer-index';
import { EsSink } from './es/es.sink';
import { IncrementalLoop } from './incremental/incremental.loop';
import { CustomerSource } from './source/customer-source';
import { OutboxReader } from './source/outbox-reader';
import { StreamSink } from './stream/stream.sink';

@Module({
  imports: [TypeOrmModule.forRoot(dataSourceOptions)],
  providers: [
    esClientProvider,
    CustomerIndex,
    EsSink,
    StreamSink,
    CustomerSource,
    OutboxReader,
    CheckpointRepository,
    ControlRepository,
    BackfillLoop,
    IncrementalLoop,
  ],
})
export class PipelineModule {}
