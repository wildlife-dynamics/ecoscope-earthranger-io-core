#!/bin/bash
set -euo pipefail

for file in /tmp/ecoscope-earthranger-io-core/release/artifacts/**/*.conda; do
    rattler-build upload prefix -c ecoscope-workflows "$file"
done
