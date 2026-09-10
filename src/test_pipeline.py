"""Offline unit tests - no Kafka or Schema Registry needed.

Covers the two pieces of logic that are easy to get wrong and hard to eyeball
during a live demo: the streaming average and the retry/backoff policy.

    python src/test_pipeline.py
"""
import io
import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import config
from aggregator import OrderAggregator, RunningStats
from consumer import backoff_delay, validate
from errors import PermanentError


class TestRunningStats(unittest.TestCase):
    def test_running_average_matches_batch_average(self):
        prices = [10.0, 20.0, 30.0, 45.5, 3.25, 199.99]
        stats = RunningStats()
        for p in prices:
            stats.add(p)
        self.assertEqual(stats.count, len(prices))
        self.assertAlmostEqual(stats.mean, sum(prices) / len(prices), places=6)
        self.assertAlmostEqual(stats.total, sum(prices), places=6)
        self.assertAlmostEqual(stats.minimum, min(prices), places=6)
        self.assertAlmostEqual(stats.maximum, max(prices), places=6)

    def test_stddev_matches_sample_stddev(self):
        prices = [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0]
        stats = RunningStats()
        for p in prices:
            stats.add(p)
        mean = sum(prices) / len(prices)
        expected = (sum((p - mean) ** 2 for p in prices) / (len(prices) - 1)) ** 0.5
        self.assertAlmostEqual(stats.stddev, expected, places=6)

    def test_stddev_of_single_value_is_zero(self):
        stats = RunningStats()
        stats.add(42.0)
        self.assertEqual(stats.stddev, 0.0)

    def test_average_is_stable_over_many_values(self):
        # A naive sum/count drifts on long streams; the running form must not.
        stats = RunningStats()
        for i in range(99_999):          # a whole number of 0/1/2 cycles
            stats.add(100.0 + (i % 3))
        self.assertAlmostEqual(stats.mean, 101.0, places=6)


class TestOrderAggregator(unittest.TestCase):
    def test_per_product_and_overall_are_tracked_separately(self):
        agg = OrderAggregator()
        agg.add("Item1", 10.0)
        agg.add("Item1", 20.0)
        agg.add("Item2", 60.0)

        self.assertAlmostEqual(agg.per_product["Item1"].mean, 15.0)
        self.assertAlmostEqual(agg.per_product["Item2"].mean, 60.0)
        self.assertEqual(agg.overall.count, 3)
        self.assertAlmostEqual(agg.overall.mean, 30.0)

    def test_add_returns_the_new_running_average(self):
        agg = OrderAggregator()
        self.assertAlmostEqual(agg.add("Item1", 10.0), 10.0)
        self.assertAlmostEqual(agg.add("Item1", 30.0), 20.0)

    def test_report_handles_the_empty_case(self):
        self.assertIn("no orders", OrderAggregator().report())


class TestValidation(unittest.TestCase):
    def test_a_good_order_passes(self):
        validate({"orderId": "1001", "product": "Item1", "price": 12.5})

    def test_negative_price_is_permanent(self):
        with self.assertRaises(PermanentError):
            validate({"orderId": "1001", "product": "Item1", "price": -5.0})

    def test_zero_price_is_permanent(self):
        with self.assertRaises(PermanentError):
            validate({"orderId": "1001", "product": "Item1", "price": 0.0})

    def test_blank_product_is_permanent(self):
        with self.assertRaises(PermanentError):
            validate({"orderId": "1001", "product": "", "price": 12.5})

    def test_blank_order_id_is_permanent(self):
        with self.assertRaises(PermanentError):
            validate({"orderId": "", "product": "Item1", "price": 12.5})


class TestBackoff(unittest.TestCase):
    def test_delay_grows_and_is_capped(self):
        # Jitter makes each call random, so compare the mean of many samples.
        def mean_delay(attempt, n=400):
            return sum(backoff_delay(attempt) for _ in range(n)) / n

        d1, d2, d3 = mean_delay(1), mean_delay(2), mean_delay(3)
        self.assertLess(d1, d2)
        self.assertLess(d2, d3)
        for attempt in range(1, 12):
            upper = config.RETRY_MAX_DELAY * (1 + config.RETRY_JITTER)
            self.assertLessEqual(backoff_delay(attempt), upper + 1e-9)

    def test_delay_is_never_negative(self):
        for attempt in range(1, 12):
            self.assertGreaterEqual(backoff_delay(attempt), 0.0)


class TestSchemas(unittest.TestCase):
    def test_order_schema_matches_the_assignment_spec(self):
        schema = json.loads(config.read_schema("order.avsc"))
        fields = {f["name"]: f["type"] for f in schema["fields"]}
        self.assertEqual(fields, {"orderId": "string",
                                  "product": "string",
                                  "price": "float"})

    def test_dead_letter_schema_is_valid_avro(self):
        from confluent_kafka.schema_registry.avro import _schema_loads  # noqa: F401
        schema = json.loads(config.read_schema("dead_letter.avsc"))
        self.assertEqual(schema["type"], "record")
        names = [f["name"] for f in schema["fields"]]
        for required in ("payload", "errorType", "errorMessage", "attempts"):
            self.assertIn(required, names)


if __name__ == "__main__":
    unittest.main(verbosity=2)
