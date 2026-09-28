#!/usr/bin/env bash
# Simply running the benchmarks with `cargo bench` seems to result in inconsistent results. Running
# the benchmarks individually with a short break (`sleep 2`) in between seems to resolve the issue.

for bench in $(cargo bench --quiet -- --list 2>/dev/null | rg "benchmark\s*\$" | sed 's/: benchmark//g')
do
    cargo bench "$bench"
    sleep 2
done
