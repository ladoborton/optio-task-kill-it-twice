function required(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`Missing env var ${name}`);
  return value;
}

export const env = {
  databaseUrl: required('DATABASE_URL'),
  esUrl: required('ES_URL'),
  rabbitMgmtUrl: required('RABBITMQ_MGMT_URL'),
  rabbitUser: required('RABBITMQ_USER'),
  rabbitPassword: required('RABBITMQ_PASSWORD'),
  // Compose project whose containers verify is allowed to kill/stop.
  composeProject: required('COMPOSE_PROJECT'),
  seedCount: Number(process.env.SEED_COUNT ?? 1_000_000),
  apiUrl: required('API_URL'),
  pipelineMetricsUrl: required('PIPELINE_METRICS_URL'),
};
