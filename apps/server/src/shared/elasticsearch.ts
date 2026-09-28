import { Client } from '@elastic/elasticsearch';
import { Provider } from '@nestjs/common';
import { config } from './config';

export const ES_CLIENT = Symbol('ES_CLIENT');

export const esClientProvider: Provider = {
  provide: ES_CLIENT,
  useFactory: () =>
    new Client({
      node: config.esUrl,
      // The client's own silent retries would hide outages from our backoff and metrics
      // (SPEC §7.2): every retry must go through one visible, bounded mechanism.
      maxRetries: 0,
      requestTimeout: config.esRequestTimeoutMs,
    }),
};
