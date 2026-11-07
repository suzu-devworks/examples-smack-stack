#!/bin/bash
script_dir=$(realpath "$(dirname "$0")")
echo "Running all CQL scripts in $script_dir"

for f in "${script_dir}"/*.cql; do
    echo "Executing $f"
    cqlsh -f "$f" || break;
done
