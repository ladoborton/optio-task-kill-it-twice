export type Status = 'PASS' | 'FAIL';

export interface CheckResult {
  status: Status;
  detail: string;
  /** Optional per-step lines printed under the report line. */
  steps?: string[];
}

export interface Check {
  id: string;
  title: string;
  run(): Promise<CheckResult>;
}

export const pass = (detail: string, steps?: string[]): CheckResult => ({ status: 'PASS', detail, steps });
export const fail = (detail: string, steps?: string[]): CheckResult => ({ status: 'FAIL', detail, steps });
