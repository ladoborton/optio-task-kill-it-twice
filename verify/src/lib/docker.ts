import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { env } from './env';

const run = promisify(execFile);

// verify talks to the host Docker daemon through the mounted socket (SPEC §11).
export async function docker(...args: string[]): Promise<string> {
  const { stdout } = await run('docker', args, { timeout: 60_000 });
  return stdout.trim();
}

// Resolve a compose service to its container id via compose labels, so verify never
// touches containers outside this project.
export async function containerOf(service: string): Promise<string> {
  const id = await docker(
    'ps', '-a', '-q',
    '--filter', `label=com.docker.compose.project=${env.composeProject}`,
    '--filter', `label=com.docker.compose.service=${service}`,
  );
  if (!id) throw new Error(`No container for service "${service}" in project "${env.composeProject}"`);
  return id.split('\n')[0];
}

/** SIGKILL: no shutdown hooks, no flushing — the crash from the assignment. */
export const kill = async (service: string) => docker('kill', await containerOf(service));
/** SIGTERM with grace period: a clean stop, used to set up a known state. */
export const stop = async (service: string) => docker('stop', await containerOf(service));
export const start = async (service: string) => docker('start', await containerOf(service));

/** Parsed JSON log lines of a service since `sinceIso` (non-JSON lines are skipped). */
export async function jsonLogsSince(service: string, sinceIso: string): Promise<Record<string, unknown>[]> {
  const { stdout, stderr } = await run('docker', ['logs', '--since', sinceIso, await containerOf(service)], {
    timeout: 60_000,
    maxBuffer: 64 * 1024 * 1024,
  });
  return `${stdout}\n${stderr}`
    .split('\n')
    .filter((l) => l.startsWith('{'))
    .map((l) => {
      try {
        return JSON.parse(l) as Record<string, unknown>;
      } catch {
        return {};
      }
    });
}
