import { Check, fail } from '../types';
import { g1 } from './g1-resume-after-kill';
import { g2 } from './g2-no-duplicates';
import { g4 } from './g4-partial-batch';

// Placeholders: each gate is implemented in its slice (gate first, then feature).
const notImplemented = (id: string, title: string, slice: string): Check => ({
  id,
  title,
  run: async () => fail(`not implemented yet (slice ${slice})`),
});

export const gates: Check[] = [
  g1,
  g2,
  notImplemented('G3', 'sink outage', 'S6'),
  g4,
  notImplemented('G5', 'observability', 'S7'),
];
