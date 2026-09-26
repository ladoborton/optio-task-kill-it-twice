import { preflight } from './checks/preflight';
import { gates } from './gates';
import { db } from './lib/db';
import { Check, CheckResult, fail } from './types';

// Usage: verify [preflight | G1 | G2 ...]  — no args runs preflight + all gates.
async function main(): Promise<number> {
  const wanted = process.argv.slice(2).map((a) => a.toUpperCase());
  const selected = wanted.length ? gates.filter((g) => wanted.includes(g.id)) : gates;
  const onlyPreflight = wanted.length === 1 && wanted[0] === 'PREFLIGHT';

  const results: [Check, CheckResult][] = [];
  const pre = await safeRun(preflight);
  results.push([preflight, pre]);
  print(preflight, pre);

  if (!onlyPreflight) {
    for (const gate of selected) {
      // Gates rely on a healthy, seeded system; running them on a broken one only adds noise.
      const r = pre.status === 'PASS' ? await safeRun(gate) : fail('skipped: preflight failed');
      results.push([gate, r]);
      print(gate, r);
    }
  }

  const failed = results.filter(([, r]) => r.status === 'FAIL').length;
  console.log(`\n${results.length - failed} passed, ${failed} failed`);
  return failed ? 1 : 0;
}

async function safeRun(check: Check): Promise<CheckResult> {
  try {
    return await check.run();
  } catch (e) {
    return fail(`error: ${(e as Error).message}`);
  }
}

function print(check: Check, r: CheckResult): void {
  const label = `${check.id === 'preflight' ? '' : `${check.id} `}${check.title} `;
  console.log(`${label.padEnd(34, '.')} ${r.status} (${r.detail})`);
  for (const line of r.steps ?? []) console.log(`    ${line}`);
}

main()
  .then(async (code) => {
    await db.end();
    process.exit(code);
  })
  .catch((e) => {
    console.error(e);
    process.exit(2);
  });
