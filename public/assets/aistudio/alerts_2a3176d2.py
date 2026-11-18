"""Sliding-window alert rule: a route is flagged when most recent feedback is negative."""
import time
from collections import Counter, defaultdict, deque
from datetime import datetime, timezone


class AlertDetector:
    def __init__(self, window_sec, min_msgs, neg_ratio, cooldown_sec):
        self.window = window_sec
        self.min_msgs = min_msgs
        self.neg_ratio = neg_ratio
        self.cooldown = cooldown_sec
        self._events = defaultdict(deque)   # route -> deque[(time, sentiment, topic)]
        self._last_alert = {}

    def add(self, route, sentiment, topic, now=None):
        """Record one analysed message; return an alert dict if the rule fires."""
        now = time.time() if now is None else now
        events = self._events[route]
        events.append((now, sentiment, topic))
        while events and now - events[0][0] > self.window:
            events.popleft()

        if len(events) < self.min_msgs:
            return None
        if now - self._last_alert.get(route, float("-inf")) < self.cooldown:
            return None

        negative_topics = [t for _, s, t in events if s == "negative"]
        ratio = len(negative_topics) / len(events)
        if ratio < self.neg_ratio:
            return None

        self._last_alert[route] = now
        return {
            "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "route": route,
            "negative_ratio": round(ratio, 2),
            "messages": len(events),
            "top_topic": Counter(negative_topics).most_common(1)[0][0],
        }
