#!/usr/bin/env sh
# Generates SEED_COUNT (default 1,000,000) customers and resets pipeline state.
# Usage: ./seed.sh     or     SEED_COUNT=100000 ./seed.sh
set -e
cd "$(dirname "$0")"
# Seed resets checkpoints, the index, the consumer projection and the queue; a pipeline or
# consumer running at the same time would write into the gap. Stop both first (D-001).
docker compose stop pipeline consumer
docker compose --profile tools run --rm -T --build seed
docker compose up -d pipeline consumer
