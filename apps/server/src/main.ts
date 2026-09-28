import 'reflect-metadata';
import { config } from './shared/config';
import { runPipeline } from './roles/pipeline/pipeline.main';
import { log } from './shared/logger';
import { migrate } from './tools/migrate';
import { seed } from './tools/seed';

// One image, started in different roles via ROLE (SPEC §5.1). consumer/api arrive in later
// slices; migrate and seed are one-shot tools.
const roles: Record<string, () => Promise<void>> = { migrate, seed, pipeline: runPipeline };

async function main(): Promise<void> {
  const run = roles[config.role];
  if (!run) throw new Error(`Unknown ROLE "${config.role}". Known: ${Object.keys(roles).join(', ')}`);
  await run();
}

main().catch((e: Error) => {
  log('fatal', { error: e.message, stack: e.stack }, 'error');
  process.exit(1);
});
