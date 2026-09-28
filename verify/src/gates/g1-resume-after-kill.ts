import { Check, fail, pass } from '../types';
import { scalar } from '../lib/db';
import { jsonLogsSince, kill, start, stop } from '../lib/docker';
import { countDocs, deleteIndex } from '../lib/es';
import { backfillCompleted, backfillPosition, fmt, MIN, resetBackfill } from '../lib/state';
import { sleep, waitFor } from '../lib/wait';

// SPEC §11 G1: docker kill mid-backfill; after restart it continues from the checkpoint —
// neither from zero nor past it — and every source row ends up in the index.
export const g1: Check = {
  id: 'G1',
  title: 'resume after kill',
  async run() {
    const steps: string[] = [];
    const sourceRows = Number(await scalar<string>('SELECT count(*) FROM customers'));
    const maxId = Number(await scalar<string>('SELECT max(id) FROM customers'));

    // 1. Known starting state: backfill from zero into an empty index. Without an empty index,
    //    "all rows are in the index" would pass even if the restarted pipeline did nothing.
    await stop('pipeline');
    await resetBackfill();
    await deleteIndex();
    await start('pipeline');
    steps.push(`reset: checkpoint 0, empty index, ${fmt(sourceRows)} source rows`);

    // 2. Let it get well into the run.
    const target = Math.floor(maxId * 0.3);
    await waitFor('backfill to reach 30%', async () => ((await backfillPosition()) >= target ? true : undefined), 10 * MIN);

    // 3. The crash: SIGKILL, no chance to flush or checkpoint anything.
    await kill('pipeline');
    const killedAt = await backfillPosition();
    const docsAtKill = await countDocs();
    steps.push(`killed: checkpoint ${fmt(killedAt)}, index had ${fmt(docsAtKill)} docs`);

    // 4. A dead pipeline must not move the checkpoint.
    await sleep(2_000);
    const stillAt = await backfillPosition();
    if (stillAt !== killedAt) return fail(`checkpoint moved while pipeline was dead (${fmt(killedAt)} -> ${fmt(stillAt)})`, steps);

    // 5. Restart; the pipeline logs where it resumes from.
    const since = new Date().toISOString();
    await start('pipeline');
    const resumedFrom = await waitFor(
      'a backfill.started log line',
      async () => {
        const line = (await jsonLogsSince('pipeline', since)).find((l) => l.event === 'backfill.started');
        return line ? Number(line.from) : undefined;
      },
      MIN,
      500,
    );
    steps.push(`restarted: resumed from ${fmt(resumedFrom)}`);
    if (resumedFrom === 0) return fail('restarted from zero', steps);
    if (resumedFrom !== killedAt) return fail(`resumed from ${fmt(resumedFrom)}, checkpoint was ${fmt(killedAt)}`, steps);

    // 6. Run to completion; nothing may be missing.
    await waitFor('backfill to complete', async () => ((await backfillCompleted()) ? true : undefined), 15 * MIN, 1_000);
    const docs = await countDocs();
    steps.push(`completed: ${fmt(sourceRows)} source / ${fmt(docs)} index`);
    if (docs !== sourceRows) return fail(`${fmt(sourceRows - docs)} rows missing from the index`, steps);

    return pass(`killed at ${fmt(docsAtKill)} / resumed at ${fmt(resumedFrom)}, 0 lost`, steps);
  },
};
