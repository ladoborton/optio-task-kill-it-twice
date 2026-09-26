import { dataSource } from '../database/data-source';
import { log } from '../shared/logger';

// ROLE=migrate: the one-shot `migrate` compose service (SPEC §5.1). Runs before any role starts,
// so only one process ever migrates.
export async function migrate(): Promise<void> {
  await dataSource.initialize();
  try {
    const applied = await dataSource.runMigrations({ transaction: 'all' });
    log('migrations.done', { applied: applied.map((m) => m.name) });
  } finally {
    await dataSource.destroy();
  }
}
