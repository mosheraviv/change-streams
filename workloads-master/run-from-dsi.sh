#!/usr/bin/env bash
set -eou pipefail

cd src/workloads || exit 2

if [ ! -e "../../workloads.yml" ]; then
    echo "Missing workloads.yml file in $PWD." >&2
    ls  >&2
    exit 3
fi

./run_workloads.py -c "../../workloads.yml"
mv workload_timestamps.csv ../../build/WorkloadOutput/
