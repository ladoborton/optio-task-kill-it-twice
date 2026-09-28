import { Check, fail } from '../types';
import { g1 } from './g1-resume-after-kill';

// Placeholders: each gate is implemented in its slice (gate first, then feature).
const notImplemented = (id: string, title: string, slice: string): Check => ({
  id,
  title,
  run: async () => fail(`not implemented yet (slice ${slice})`),
});

export const gates: Check[] = [
  g1,
  notImplemented('G2', 'no duplicates', 'S3/S4'),
  notImplemented('G3', 'sink outage', 'S6'),
  notImplemented('G4', 'partial batch failure', 'S5'),
  notImplemented('G5', 'observability', 'S7'),
];
