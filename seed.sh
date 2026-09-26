#!/usr/bin/env sh
# Generates SEED_COUNT (default 1,000,000) customers and resets pipeline state.
# Usage: ./seed.sh     or     SEED_COUNT=100000 ./seed.sh
set -e
cd "$(dirname "$0")"
exec docker compose --profile tools run --rm -T --build seed
