"""Sentiment + topic analysis for passenger feedback.

VADER (a rule-based model built for short social-media text) is extended with
a transport-specific lexicon, because words like "delayed", "packed" or
"punctual" carry little or no sentiment in a general-purpose dictionary.
"""
import re

from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# VADER valence scale: -4 (very negative) ... +4 (very positive)
DOMAIN_LEXICON = {
    # complaints
    "delayed": -2.4, "delay": -2.0, "delays": -2.0, "late": -1.6,
    "cancelled": -2.4, "canceled": -2.4, "breakdown": -2.3, "stuck": -1.8,
    "stranded": -2.2, "wait": -0.8, "waiting": -1.0,
    "overcrowded": -2.6, "crowded": -1.9, "packed": -1.6, "stuffy": -1.6,
    "filthy": -2.8, "dirty": -2.3, "smelly": -2.0, "smells": -1.8,
    "garbage": -2.0, "broken": -2.0, "unsafe": -2.6, "rash": -2.0,
    "rashly": -2.0, "rude": -2.5, "stolen": -2.2, "overcharged": -2.2,
    "rip-off": -2.6, "ripoff": -2.6, "crashing": -2.0, "crashes": -2.0,
    "unacceptable": -2.6, "useless": -2.4, "bumpy": -1.4, "hiked": -1.4,
    "unhelpful": -2.2, "uninformed": -1.8,
    # VADER treats "stop" as negative, but in this domain it is a bus stop
    "stop": 0.0, "stops": 0.0,
    # praise
    "punctual": 2.4, "spotless": 2.7, "clean": 1.9, "smooth": 1.7,
    "comfortable": 2.1, "polite": 2.1, "helpful": 2.1, "reliable": 2.2,
    "accurate": 1.8, "quick": 1.5, "affordable": 1.8, "reasonable": 1.4,
    "hassle-free": 2.0, "peaceful": 1.8,
}

# Multi-word expressions VADER can't score directly are rewritten first.
PHRASES = [
    (r"\bon[- ]time\b", "punctual"),
    (r"\bpacked like sardines\b", "overcrowded"),
    (r"\bno (?:space|room|seats?)\b", "overcrowded"),
    (r"\brip[- ]off\b", "rip-off"),
    (r"\blate[- ]night\b", "night"),
    (r"\bnobody (?:helped|cares|came)\b", "unhelpful"),
    (r"\b(?:can'?t|cannot|couldn'?t) stand\b", "hate"),
    (r"\b(?:not|wasn'?t|isn'?t|never|doesn'?t|didn'?t) work(?:ing|s|ed)?\b", "broken"),
    (r"\bno (?:announcements?|updates?|information)\b", "uninformed"),
]

# Keyword rules for "what is this complaint/compliment about?"
TOPICS = {
    "Delay & punctuality": r"delay|\blate\b|\bwait|punctual|on[- ]time|schedule|cancel|breakdown|stuck|stranded|frequen",
    "Safety & security": r"unsafe|\bsafe|safety|rash|reckless|danger|harass|theft|stolen|accident|lighting|\bdark",
    "Overcrowding": r"crowd|packed|sardines|no (?:space|room|seats?)|\bseats?\b|standing|rush hour|congest",
    "Cleanliness": r"clean|dirt|filth|smell|garbage|\bbins?\b|litter|toilet|stink|spotless|hygien",
    "Staff behaviour": r"driver|conductor|staff|guard|rude|polite|helpful|behaviou?r|attitude",
    "Fares & ticketing": r"\bfares?\b|ticket|price|\bpass\b|smart ?card|top-?up|rip-?off|refund|charge|expensive|afford",
    "Comfort & facilities": r"\bAC\b|air[- ]?con|seat|comfort|ride|smooth|bumpy|noise|stuffy|ventilat|facilit|escalator|\blift\b",
    "App & information": r"\bapp\b|tracking|announce|information|update|display|website|signage|real-?time|timetable",
}

POSITIVE_THRESHOLD = 0.05   # standard VADER cut-offs
NEGATIVE_THRESHOLD = -0.05

_analyzer = SentimentIntensityAnalyzer()
_analyzer.lexicon.update(DOMAIN_LEXICON)
_topic_res = {t: re.compile(p, re.IGNORECASE) for t, p in TOPICS.items()}


def _normalise(text: str) -> str:
    for pattern, replacement in PHRASES:
        text = re.sub(pattern, replacement, text, flags=re.IGNORECASE)
    return text


def classify_topic(text: str) -> str:
    """Topic with the most keyword hits; ties go to the topic listed first in TOPICS."""
    scores = {t: len(rx.findall(text)) for t, rx in _topic_res.items()}
    best = max(scores, key=scores.get)
    return best if scores[best] else "General"


def analyze(text: str) -> dict:
    """Return {'sentiment': positive|neutral|negative, 'score': -1..1, 'topic': str}."""
    compound = _analyzer.polarity_scores(_normalise(text))["compound"]
    if compound >= POSITIVE_THRESHOLD:
        label = "positive"
    elif compound <= NEGATIVE_THRESHOLD:
        label = "negative"
    else:
        label = "neutral"
    return {"sentiment": label, "score": round(compound, 4), "topic": classify_topic(text)}
