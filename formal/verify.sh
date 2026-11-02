#!/usr/bin/env bash
set -euo pipefail
cd -- "$(dirname -- "$0")"
lean --version
lean -o Connectors.olean Connectors.lean
LEAN_PATH="$PWD${LEAN_PATH:+:$LEAN_PATH}" lean Audit.lean
