import { Client } from '@elastic/elasticsearch';
import { Controller, Get, Inject, NotFoundException, Param, Query } from '@nestjs/common';
import { DataSource } from 'typeorm';
import { config } from '../../../shared/config';
import { ES_CLIENT } from '../../../shared/elasticsearch';

const PAGE_SIZE = 20;

/** SPEC §9 data: what the pipeline replicated, and the changes flowing through it. */
@Controller('api')
export class DataController {
  constructor(
    private readonly db: DataSource,
    @Inject(ES_CLIENT) private readonly es: Client,
  ) {}

  /** Search the replicated index — the view a segmentation service would have. */
  @Get('customers')
  async search(@Query('q') q = '', @Query('segment') segment = '', @Query('city') city = '', @Query('page') page = '1') {
    const filter = [
      ...(segment ? [{ term: { segment } }] : []),
      ...(city ? [{ term: { city } }] : []),
    ];
    const must = q ? [{ multi_match: { query: q, fields: ['name', 'email'], operator: 'and' as const } }] : [];
    const from = (Math.max(Number(page) || 1, 1) - 1) * PAGE_SIZE;
    const res = await this.es.search({
      index: config.esIndex,
      from,
      size: PAGE_SIZE,
      version: true,
      track_total_hits: true,
      sort: q ? undefined : [{ id: 'asc' }],
      query: { bool: { must, filter } },
    });
    const total = typeof res.hits.total === 'number' ? res.hits.total : res.hits.total?.value ?? 0;
    return { total, page: Number(page) || 1, page_size: PAGE_SIZE, items: res.hits.hits.map((h) => h._source) };
  }

  /**
   * One customer in all three places side by side: the source row, the index document, the consumer's
   * projection — so an operator can see whether and where a change has arrived.
   */
  @Get('customers/:id')
  async detail(@Param('id') id: string) {
    if (!/^\d+$/.test(id)) throw new NotFoundException();
    const [[source], [projection], index, dlq] = await Promise.all([
      this.db.query(`SELECT id, email, name, city, segment, balance, attributes, version, updated_at FROM customers WHERE id = $1`, [id]),
      this.db.query(`SELECT id, version, deleted, email, city, segment, source, applied_at FROM consumer.customers WHERE id = $1`, [id]),
      // GET returns _version, which with external versioning is the customers.version indexed.
      this.es.get({ index: config.esIndex, id })
        .then((d) => ({ ...(d._source as object), _version: d._version }))
        .catch(() => null),
      this.db.query(`SELECT id, version, status, attempts, error_type, error_reason FROM dlq_records WHERE customer_id = $1 ORDER BY id DESC`, [id]),
    ]);
    if (!source && !projection && !index) throw new NotFoundException();
    return { source: source ?? null, index, consumer: projection ?? null, dlq };
  }

  /** Latest outbox entries, each marked shipped or not relative to the incremental checkpoint. */
  @Get('changes')
  changes(@Query('limit') limit = '30') {
    return this.db.query(
      `SELECT o.seq, o.txid, o.customer_id, o.op, o.version, o.created_at,
              (o.txid, o.seq) <= (c.position_txid, c.position) AS shipped
         FROM customer_changes o
         JOIN pipeline_checkpoints c ON c.stream = 'incremental'
        ORDER BY o.seq DESC
        LIMIT $1`,
      [Math.min(Number(limit) || 30, 200)],
    );
  }
}
