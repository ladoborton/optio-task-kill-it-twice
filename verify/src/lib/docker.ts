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
