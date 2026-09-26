#!/usr/bin/env sh
# Runs the gate checks inside a container; the host needs only Docker.
# Usage: ./verify.sh            preflight + all gates
#        ./verify.sh G1         preflight + one gate
#        ./verify.sh preflight  preflight only
set -e
cd "$(dirname "$0")"
exec docker compose --profile tools run --rm -T --build verify "$@"
