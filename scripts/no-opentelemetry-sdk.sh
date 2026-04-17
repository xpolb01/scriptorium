#!/bin/bash
# Lint guard: prevent accidental import of opentelemetry-sdk.
#
# We implement OTel data models in-process without the opentelemetry-sdk crate
# (Rust OTel logging still beta; custom tracing::Layer gives better control over
# SQLite write path and schema fidelity).
#
# This script greps all Cargo.toml files and fails if "opentelemetry" is found
# in a dependency (ignoring comment lines and this script itself in prose).
#
# Exit code: 0 if no opentelemetry deps found, 1 if any found.
#
# To bypass (if a legitimate use case arises): grep patterns can be refined to
# exclude specific contexts, but adding opentelemetry-sdk should first be
# discussed and documented in CLAUDE.md or the plan.

set -e

cd "$(dirname "$0")/.."

# Find all Cargo.toml files, strip comments, and grep for opentelemetry
# (excluding lines starting with # and blank lines).
found=$(grep -r "opentelemetry" Cargo.toml crates/*/Cargo.toml 2>/dev/null | grep -v "^[[:space:]]*#" | wc -l)

if [ "$found" -gt 0 ]; then
    echo "LINT GUARD FAILED: found opentelemetry in Cargo.toml(s):"
    grep -r "opentelemetry" Cargo.toml crates/*/Cargo.toml 2>/dev/null | grep -v "^[[:space:]]*#"
    exit 1
else
    echo "✓ No opentelemetry-sdk dependencies found"
    exit 0
fi
