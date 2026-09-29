import { DataSource, DataSourceOptions } from 'typeorm';
import { config } from '../shared/config';
import { Customer } from './entities/customer.entity';
import { CustomerChange } from './entities/customer-change.entity';
import { DlqRecord } from './entities/dlq-record.entity';
import { PipelineCheckpoint } from './entities/pipeline-checkpoint.entity';
import { PipelineControl } from './entities/pipeline-control.entity';
import { PipelineHeartbeat } from './entities/pipeline-heartbeat.entity';
import { InitialSchema1727164800000 } from './migrations/1727164800000-initial-schema';
import { ConsumerProjection1727400000000 } from './migrations/1727400000000-consumer-projection';
import { OutboxTxid1727500000000 } from './migrations/1727500000000-outbox-txid';
import { DlqReplayRequest1727600000000 } from './migrations/1727600000000-dlq-replay-request';
import { HeartbeatDetails1727700000000 } from './migrations/1727700000000-heartbeat-details';

// Migrations are listed explicitly (no glob), so the same code works from ts and compiled js.
// The options are shared by the one-shot tools (plain DataSource) and the Nest roles (TypeOrmModule).
export const dataSourceOptions: DataSourceOptions = {
  type: 'postgres',
  url: config.databaseUrl,
  entities: [Customer, CustomerChange, DlqRecord, PipelineCheckpoint, PipelineControl, PipelineHeartbeat],
  migrations: [
    InitialSchema1727164800000,
    ConsumerProjection1727400000000,
    OutboxTxid1727500000000,
    DlqReplayRequest1727600000000,
    HeartbeatDetails1727700000000,
  ],
  // SPEC §5.6 rule 3: schema changes only through migrations.
  synchronize: false,
  migrationsRun: false,
  logging: ['error'],
};

export const dataSource = new DataSource(dataSourceOptions);
