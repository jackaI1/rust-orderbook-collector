"""Minute ranges isolate timer output from coincident stream snapshots."""
import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location("live_validation", Path(__file__).parents[1] / "scripts/validate_live.py")
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class MinuteRangeTests(unittest.TestCase):
    def test_exclusive_ranges_ignore_adjacent_stream_snapshots(self):
        ranges = [(5, 8, 1000), (12, 14, 2000)]
        starts = [5, 12]
        self.assertIsNone(module.ordinal_batch(ranges, starts, 4))
        self.assertEqual(module.ordinal_batch(ranges, starts, 5), 1000)
        self.assertEqual(module.ordinal_batch(ranges, starts, 7), 1000)
        self.assertIsNone(module.ordinal_batch(ranges, starts, 8))
        self.assertEqual(module.ordinal_batch(ranges, starts, 12), 2000)
        self.assertIsNone(module.ordinal_batch(ranges, starts, 14))

    def test_empty_discovery_minutes_never_capture_records(self):
        ranges = [(0, 0, 1000), (0, 0, 2000), (9, 11, 3000)]
        self.assertIsNone(module.ordinal_batch(ranges, [0, 0, 9], 0))
        self.assertEqual(module.ordinal_batch(ranges, [0, 0, 9], 9), 3000)

    def test_no_ranges_has_no_match(self):
        self.assertIsNone(module.ordinal_batch([], [], 0))
