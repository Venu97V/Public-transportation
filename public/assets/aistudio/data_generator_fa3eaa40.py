"""Synthetic passenger feedback, used when no real dataset is supplied.

Every message comes with the label it was written to express, so the same
generator can also be used to sanity-check the sentiment model (evaluate.py).
"""
import random
import uuid
from datetime import datetime, timezone

ROUTE_MODE = {
    "Bus 12": "bus",
    "Bus 45": "bus",
    "Bus 101": "bus",
    "Metro Line A": "metro",
    "Metro Line B": "metro",
    "Suburban Line 1": "train",
    "Suburban Line 2": "train",
}
ROUTES = list(ROUTE_MODE)
SOURCES = ["twitter", "app-review", "feedback-form", "helpline"]

POSITIVE = [
    "{route} was right on time this morning, great job!",
    "Punctual as ever, {route} never lets me down",
    "Reached the office early thanks to a super quick {mode} ride",
    "Plenty of seats on {route} this evening, so relaxing",
    "Not crowded at all today, finally a peaceful commute 😊",
    "The {mode} was spotless today, really impressed",
    "Clean seats and a fresh coach. Well done!",
    "The driver was very polite and helped an elderly passenger",
    "Station staff on {route} were so helpful, solved my ticket issue quickly",
    "Smart card top-up took seconds and the fares are reasonable",
    "Affordable fares and a hassle-free ticket system 👍",
    "Live tracking for {route} showed accurate arrival times, love it",
    "The new app makes planning my {mode} trip so easy",
    "AC works great on {route}, the ride was smooth and comfortable",
    "Comfortable seats and a smooth ride today",
    "Felt safe at the stop after dark, good lighting and helpful guards",
]

NEGATIVE_DELAY = [
    "{route} delayed by 40 minutes again, absolutely unacceptable",
    "Waiting for {route} for an hour with no updates. Terrible service 😡",
    "Another breakdown on {route}, stuck in the middle of nowhere",
    "{route} cancelled without any notice, so frustrating",
    "Late again! {route} is never on time anymore",
    "So overcrowded, couldn't even breathe inside the {mode}",
    "Packed like sardines on {route}, no space to stand",
    "Rush hour on {route} is a nightmare, completely crowded",
]
NEGATIVE_OTHER = [
    "Seats are filthy and the coach smells awful",
    "Dirty station and overflowing bins. Disgusting",
    "The {mode} floor was covered in garbage, disgusting",
    "Felt unsafe at the stop after dark, no lighting at all",
    "Driver of {route} was driving rashly, really dangerous",
    "Phone stolen on a crowded {mode}, security is a joke",
    "Conductor was rude and refused to give change",
    "Driver ignored passengers waving at the stop, so rude",
    "Ticket machine took my money and nobody helped",
    "Fares hiked again, total rip-off 😤",
    "Overcharged on my smart card and still no refund",
    "The app keeps crashing and live tracking is completely wrong",
    "Information display shows wrong timings again, useless",
    "AC is broken on {route} again, sweating the whole ride",
    "Terrible bumpy ride and uncomfortable seats",
]
NEGATIVE = NEGATIVE_DELAY + NEGATIVE_OTHER

NEUTRAL = [
    "{route} ran on its usual schedule today",
    "Took the {mode} to the city centre at 9 am",
    "Is there a service on {route} on Sunday?",
    "Platform change announced for the 6:15 train",
    "Bus pass renewal counter opens at 10",
    "Which stop is closest to the central library on {route}?",
    "Boarded {route} near the market around noon",
    "Timetable for {route} updated on the website",
]

TAGS = ["", "", "", " #commute", " #publictransport", " @transit_help"]


def make_record(text, route=None, source=None, timestamp=None, rng=random):
    """Wrap a piece of text in the JSON structure published to Kafka."""
    route = route or rng.choice(ROUTES)
    return {
        "id": str(uuid.uuid4()),
        "timestamp": timestamp
        or datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "source": source or rng.choice(SOURCES),
        "mode": ROUTE_MODE.get(route, "bus"),
        "route": route,
        "text": text,
    }


class FeedbackGenerator:
    """Endless stream of (record, intended_label).

    With incidents=True a random route occasionally suffers a burst of
    complaints, which is what the real-time alert rule is meant to catch.
    """

    def __init__(self, incidents=True, seed=None):
        self.rng = random.Random(seed)
        self.incidents = incidents
        self._incident_route = None
        self._incident_left = 0

    def next(self):
        rng = self.rng
        route = rng.choice(ROUTES)
        sentiment = rng.choices(
            ["positive", "neutral", "negative"], weights=[35, 30, 35]
        )[0]
        pool = {"positive": POSITIVE, "neutral": NEUTRAL, "negative": NEGATIVE}

        if self.incidents:
            if self._incident_left == 0 and rng.random() < 0.003:
                self._incident_route = rng.choice(ROUTES)
                self._incident_left = 40
            if self._incident_left > 0:
                self._incident_left -= 1
                if rng.random() < 0.75:
                    route, sentiment = self._incident_route, "negative"
                    pool["negative"] = NEGATIVE_DELAY

        text = rng.choice(pool[sentiment]).format(
            route=route, mode=ROUTE_MODE[route]
        ) + rng.choice(TAGS)
        return make_record(text, route=route, rng=rng), sentiment
