#!/usr/bin/env bash
# Offline demo on the bundled OCI-DI fixture workspace: analyze -> migrate -> verify -> publish (dry run).
# Nothing here contacts OCI-DI or AIDP.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
OUT="${1:-/tmp/ocidi-demo}"
PY="${PYTHON:-python3}"
export PYTHONPATH="$HERE${PYTHONPATH:+:$PYTHONPATH}"
rm -rf "$OUT"
"$PY" -m ocidi2aidp analyze "$HERE/tests/fixtures/snapshot_sales" -o "$OUT/analysis"
"$PY" -m ocidi2aidp migrate "$HERE/tests/fixtures/snapshot_sales" -o "$OUT/out"
"$PY" -m ocidi2aidp fallback list "$OUT/out"
"$PY" -m ocidi2aidp verify "$OUT/out" || true
"$PY" -m ocidi2aidp publish "$OUT/out"
echo
echo "Read: $OUT/analysis/analysis.md and $OUT/out/REVIEW.md"
