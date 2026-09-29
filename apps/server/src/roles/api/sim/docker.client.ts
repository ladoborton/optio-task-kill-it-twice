import { BadRequestException, Injectable } from '@nestjs/common';
import { request } from 'node:http';
import { config } from '../../../shared/config';

/** Services the simulation may stop/start/kill. The api (and the databases it needs) are not on the list. */
export const SIMULATABLE = ['elasticsearch', 'rabbitmq', 'pipeline', 'consumer'] as const;
export type SimService = (typeof SIMULATABLE)[number];
export type SimAction = 'stop' | 'start' | 'kill';

/**
 * Minimal Docker Engine API client over the mounted socket (SPEC §9 simulation).
 * DEV ONLY: whoever can reach this api can stop containers on the host. Acceptable for a local
 * demo stack; it would never be deployed like this.
 */
@Injectable()
export class DockerClient {
  async act(service: string, action: string): Promise<void> {
    if (!SIMULATABLE.includes(service as SimService)) throw new BadRequestException(`service must be one of ${SIMULATABLE.join(', ')}`);
    if (!['stop', 'start', 'kill'].includes(action)) throw new BadRequestException('action must be stop | start | kill');
    const id = await this.containerOf(service);
    await this.call('POST', `/containers/${id}/${action}`);
  }

  async states(): Promise<Record<string, string>> {
    const filters = encodeURIComponent(JSON.stringify({ label: [`com.docker.compose.project=${config.composeProject}`] }));
    const list = (await this.call('GET', `/containers/json?all=1&filters=${filters}`)) as { Labels: Record<string, string>; State: string }[];
    return Object.fromEntries(
      list
        .map((c) => [c.Labels['com.docker.compose.service'], c.State] as const)
        .filter(([s]) => SIMULATABLE.includes(s as SimService)),
    );
  }

  private async containerOf(service: string): Promise<string> {
    const filters = encodeURIComponent(JSON.stringify({
      label: [`com.docker.compose.project=${config.composeProject}`, `com.docker.compose.service=${service}`],
    }));
    const [c] = (await this.call('GET', `/containers/json?all=1&filters=${filters}`)) as { Id: string }[];
    if (!c) throw new BadRequestException(`no container for ${service}`);
    return c.Id;
  }

  private call(method: string, path: string): Promise<unknown> {
    return new Promise((resolve, reject) => {
      const req = request({ socketPath: '/var/run/docker.sock', method, path, timeout: 60_000 }, (res) => {
        let body = '';
        res.on('data', (chunk) => (body += chunk));
        res.on('end', () => {
          // 304 = already in that state (e.g. start on a running container): fine for a simulation.
          if (res.statusCode && res.statusCode >= 400) return reject(new Error(`docker ${method} ${path} -> ${res.statusCode} ${body}`));
          resolve(body ? JSON.parse(body) : null);
        });
      });
      req.on('error', reject);
      req.end();
    });
  }
}
