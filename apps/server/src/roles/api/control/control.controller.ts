import { BadRequestException, Body, Controller, HttpCode, Param, Post, Put } from '@nestjs/common';
import { DataSource } from 'typeorm';

const int = (value: unknown, name: string, min: number, max: number): number => {
  const n = Number(value);
  if (!Number.isInteger(n) || n < min || n > max) throw new BadRequestException(`${name} must be an integer in [${min}, ${max}]`);
  return n;
};

/**
 * SPEC §9 control. Every action only writes the DESIRED state to Postgres (pipeline_control,
 * pipeline_checkpoints); the pipeline picks it up on its next step. Nothing is sent to the pipeline
 * process, so a control action works — and survives — even while the pipeline is down.
 */
@Controller('api')
export class ControlController {
  constructor(private readonly db: DataSource) {}

  @Post('backfill/:action')
  @HttpCode(204)
  async backfill(@Param('action') action: string): Promise<void> {
    if (action === 'pause' || action === 'resume') {
      await this.db.query(`UPDATE pipeline_control SET backfill_state = $1, updated_at = now()`, [action === 'pause' ? 'paused' : 'running']);
    } else if (action === 'reset') {
      // Start over from id 0. A batch in flight notices via the compare-and-set checkpoint (D-001);
      // documents already in the index are simply rewritten (idempotent, SPEC §6.3).
      await this.db.query(`UPDATE pipeline_checkpoints SET position = 0, completed_at = NULL, updated_at = now() WHERE stream = 'backfill'`);
      await this.db.query(`UPDATE pipeline_control SET backfill_state = 'running', updated_at = now()`);
    } else throw new BadRequestException('action must be pause | resume | reset');
  }

  @Post('incremental/:action')
  @HttpCode(204)
  async incremental(@Param('action') action: string): Promise<void> {
    if (action !== 'pause' && action !== 'resume') throw new BadRequestException('action must be pause | resume');
    await this.db.query(`UPDATE pipeline_control SET incremental_state = $1, updated_at = now()`, [action === 'pause' ? 'paused' : 'running']);
  }

  @Put('settings')
  @HttpCode(204)
  async settings(@Body() body: { batch_size?: unknown; poll_interval_ms?: unknown }): Promise<void> {
    // Same bounds as the CHECK constraints on pipeline_control.
    const batchSize = int(body.batch_size, 'batch_size', 1, 10_000);
    const pollMs = int(body.poll_interval_ms, 'poll_interval_ms', 100, 60_000);
    await this.db.query(`UPDATE pipeline_control SET batch_size = $1, poll_interval_ms = $2, updated_at = now()`, [batchSize, pollMs]);
  }
}
