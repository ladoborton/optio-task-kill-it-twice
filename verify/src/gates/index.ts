import { Check } from '../types';
import { g1 } from './g1-resume-after-kill';
import { g2 } from './g2-no-duplicates';
import { g3 } from './g3-sink-outage';
import { g4 } from './g4-partial-batch';
import { g5 } from './g5-observability';

// Run order matters only for time: G1 and G2 reset and re-run the backfill; G3–G5 start from a
// completed backfill and converged sinks.
export const gates: Check[] = [g1, g2, g3, g4, g5];
