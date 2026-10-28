"""Offline evidence-calculation regressions; no sockets or subprocesses."""
import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location("health_summary", Path(__file__).parents[1] / "scripts/summarize_health.py")
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def sample(stamp, ready=96, symbols=100):
    return {"mode": "live", "received_ms": stamp, "ready": ready, "symbols": symbols}


class SummaryTests(unittest.TestCase):
    def test_empty_discovery_time_is_not_coverage(self):
        report = module.summarize([sample(0, 0, 0)])
        self.assertEqual(report["samples"], 0)
        self.assertIsNone(report["ready_ratio_max"])
        self.assertEqual(report["longest_observed_threshold_span_seconds"], 0)

    def test_low_sample_and_long_gap_break_sustained_span(self):
        report = module.summarize([sample(0), sample(10_000), sample(20_000, 94),
                                   sample(30_000), sample(60_000), sample(70_000), sample(80_000)])
        self.assertEqual(report["longest_observed_threshold_span_seconds"], 20)
        self.assertEqual(report["ready_ratio_min"], 0.94)

    def test_each_sample_uses_its_own_dynamic_denominator(self):
        report = module.summarize([sample(0, 100, 100), sample(10_000, 190, 200)])
        self.assertEqual(report["ready_ratio_max"], 1)
        self.assertEqual(report["last_ready_ratio"], 0.95)
        self.assertEqual(report["longest_observed_threshold_span_seconds"], 10)


if __name__ == "__main__":
    unittest.main()
