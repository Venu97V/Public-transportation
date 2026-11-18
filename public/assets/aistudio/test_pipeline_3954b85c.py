"""Offline unit tests (no Kafka needed):  python -m unittest -v"""
import unittest

from alerts import AlertDetector
from sentiment import analyze


class SentimentTests(unittest.TestCase):
    def test_labels(self):
        cases = {
            "Bus 12 delayed by 40 minutes again, absolutely unacceptable": "negative",
            "Packed like sardines on Metro Line A, no space to stand": "negative",
            "The metro was spotless today, really impressed": "positive",
            "Right on time and very comfortable, great job!": "positive",
            "Is there a service on Bus 12 on Sunday?": "neutral",
            "Which stop is closest to the central library?": "neutral",
        }
        for text, expected in cases.items():
            self.assertEqual(analyze(text)["sentiment"], expected, text)

    def test_negation(self):
        self.assertEqual(analyze("The bus was not on time")["sentiment"], "negative")

    def test_topics(self):
        self.assertEqual(analyze("Seats are filthy and the coach smells awful")["topic"], "Cleanliness")
        self.assertEqual(analyze("Waiting for the bus for an hour")["topic"], "Delay & punctuality")
        self.assertEqual(analyze("Conductor was rude to me")["topic"], "Staff behaviour")
        self.assertEqual(analyze("hello")["topic"], "General")


class AlertTests(unittest.TestCase):
    def make(self):
        return AlertDetector(window_sec=30, min_msgs=5, neg_ratio=0.7, cooldown_sec=45)

    def test_fires_on_negative_burst(self):
        det, alert = self.make(), None
        for i in range(6):
            alert = det.add("Bus 12", "negative", "Delay & punctuality", now=i) or alert
        self.assertIsNotNone(alert)
        self.assertEqual(alert["route"], "Bus 12")
        self.assertEqual(alert["top_topic"], "Delay & punctuality")

    def test_quiet_when_mixed(self):
        det = self.make()
        results = [det.add("Bus 12", s, "General", now=i)
                   for i, s in enumerate(["positive", "negative", "neutral", "negative", "positive", "negative"])]
        self.assertFalse(any(results))

    def test_cooldown_and_window(self):
        det = self.make()
        fired = [det.add("A", "negative", "General", now=i) for i in range(12)]
        self.assertEqual(sum(1 for f in fired if f), 1)          # cooldown suppresses repeats
        self.assertIsNone(det.add("A", "negative", "General", now=500))  # old events expired


if __name__ == "__main__":
    unittest.main()
