import { DataSource } from 'typeorm';
import { config } from '../shared/config';
import { Customer } from './entities/customer.entity';
import { CustomerChange } from './entities/customer-change.entity';
import { DlqRecord } from './entities/dlq-record.entity';
import { PipelineCheckpoint } from './entities/pipeline-checkpoint.entity';
import { PipelineControl } from './entities/pipeline-control.entity';
import { PipelineHeartbeat } from './entities/pipeline-heartbeat.entity';
import { InitialSchema1727164800000 } from './migrations/1727164800000-initial-schema';

// Migrations are listed explicitly (no glob), so the same code works from ts and compiled js.
export const dataSource = new DataSource({
  type: 'postgres',
  url: config.databaseUrl,
  entities: [Customer, CustomerChange, DlqRecord, PipelineCheckpoint, PipelineControl, PipelineHeartbeat],
  migrations: [InitialSchema1727164800000],
  // SPEC §5.6 rule 3: schema changes only through migrations.
  synchronize: false,
  migrationsRun: false,
  logging: ['error'],
});
