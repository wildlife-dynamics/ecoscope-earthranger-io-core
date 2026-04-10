#!/bin/bash
set -euo pipefail

RECIPES=("release/ecoscope-earthranger-io-core")

export ECOSCOPE_EARTHRANGER_IO_CORE_HATCH_VCS_VERSION=$(hatch version)
echo "ECOSCOPE_EARTHRANGER_IO_CORE_HATCH_VCS_VERSION=$ECOSCOPE_EARTHRANGER_IO_CORE_HATCH_VCS_VERSION"

OUTPUT_DIR=/tmp/ecoscope-earthranger-io-core/release/artifacts
rm -rf "$OUTPUT_DIR"
mkdir -p "$OUTPUT_DIR"

for rec in "${RECIPES[@]}"; do
    echo "Building $rec"
    rattler-build build \
        --recipe "$(pwd)/publish/recipes/${rec}.yaml" \
        --output-dir "$OUTPUT_DIR" \
        --channel "file://${OUTPUT_DIR}" \
        --channel https://prefix.dev/ecoscope-workflows \
        --channel conda-forge
done
