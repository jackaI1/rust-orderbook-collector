#!/usr/bin/env python3
"""Summarize observed readiness without treating a peak as sustained coverage."""
import argparse
import collections
import json
from pathlib import Path


def summarize(records, threshold=0.95, max_gap_ms=15_000):
    samples = []
    reasons = collections.Counter()
    sweeps = promotions = 0
    for record in records:
        if record.get("event") == "connection_end":
            reasons[record.get("reason", "unknown")] += 1
        if record.get("event") == "connection_promoted":
            promotions += 1
        if record.get("event") == "discovery_complete":
            sweeps += int(record.get("sweep_complete") is True)
        if record.get("mode") != "live" or record.get("symbols", 0) <= 0:
            continue
        samples.append((record["received_ms"], record["ready"] / record["symbols"],
                        record.get("coverage", {}).get("redundant_ready", 0) / record["symbols"]))
    longest = 0
    first = previous = None
    for stamp, ratio, _ in samples:
        if ratio < threshold:
            first = previous = None
            continue
        if previous is None or not 0 <= stamp - previous <= max_gap_ms:
            first = stamp
        previous = stamp
        longest = max(longest, stamp - first)
    return {
        "scope": "observed nonempty-universe health samples; no interpolation or history guarantee",
        "samples": len(samples),
        "threshold": threshold,
        "max_allowed_sample_gap_ms": max_gap_ms,
        "first_sample_ms": samples[0][0] if samples else None,
        "last_sample_ms": samples[-1][0] if samples else None,
        "ready_ratio_min": min((s[1] for s in samples), default=None),
        "ready_ratio_max": max((s[1] for s in samples), default=None),
        "last_ready_ratio": samples[-1][1] if samples else None,
        "redundant_ready_ratio_max": max((s[2] for s in samples), default=None),
        "longest_observed_threshold_span_seconds": longest / 1000,
        "complete_sweeps": sweeps,
        "warm_promotions": promotions,
        "connection_ends_by_reason": dict(reasons),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("log", type=Path)
    args = parser.parse_args()
    if args.log.stat().st_size > 16 * 1024 * 1024:
        parser.error("refusing a log larger than 16 MiB")
    records = []
    skipped = 0
    with args.log.open() as source:
        for line in source:
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                skipped += 1
                continue
            if isinstance(record, dict):
                records.append(record)
    report = summarize(records)
    report["non_json_lines_skipped"] = skipped
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
