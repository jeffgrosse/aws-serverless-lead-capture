"""
Example lead scorer - rule-based heuristic.

This is a worked example, not part of the idempotency pattern this repo
demonstrates. Swap the body of score_lead() for your own business rules (or
a model invocation) - the input contract (the `lead` dict) and output
contract ({score, tier, factors}) are what the ingest Lambda depends on, so
keep those and change everything else.
"""

import re
from typing import Any, Dict, List

BASE_SCORE = 30

# Example: weight leads by a "service interested in" field. Replace with
# whatever signals matter for your product.
SERVICE_WEIGHTS = {
    "enterprise plan": 35,
    "custom integration": 25,
    "standard plan": 10,
    "not sure yet": 5,
}

FREE_EMAIL_DOMAINS = {
    "gmail.com", "yahoo.com", "hotmail.com", "outlook.com",
    "live.com", "icloud.com", "me.com", "aol.com", "proton.me", "protonmail.com",
}

DISPOSABLE_EMAIL_DOMAINS = {
    "mailinator.com", "guerrillamail.com", "10minutemail.com",
    "tempmail.com", "trashmail.com", "yopmail.com",
}

# Keywords that signal a message has substance worth reading. Replace with
# whatever vocabulary correlates with a qualified lead in your domain.
DATA_SIGNAL_KEYWORDS = {
    "budget", "timeline", "team", "integration", "migrate", "scale",
    "users", "customers", "data", "api",
}
# Word-boundary match, not substring - a naive `kw in text` check let short
# keywords like "api" false-positive inside unrelated words ("rapidly",
# "capital"). (Note: this also means a plural like "apis" no longer matches
# "api" - a deliberate tightening, since the found bug was over-matching.)
_DATA_SIGNAL_PATTERN = re.compile(
    r"\b(?:" + "|".join(re.escape(kw) for kw in DATA_SIGNAL_KEYWORDS) + r")\b"
)


def _tier(score: int) -> str:
    if score >= 75: return "A"
    if score >= 50: return "B"
    if score >= 25: return "C"
    return "D"


def _score_service(service: str) -> tuple[int, str]:
    if not service:
        return 0, "no service selected"
    key = service.strip().lower()
    for label, weight in SERVICE_WEIGHTS.items():
        if label in key:
            return weight, f"service: {service} (+{weight})"
    return 0, f"service: {service} (unrecognized, +0)"


def _score_email(email: str) -> tuple[int, str]:
    if not email or "@" not in email:
        return 0, "email: missing or malformed"
    domain = email.split("@", 1)[1].strip().lower()
    if not domain:
        return 0, "email: missing or malformed"
    if domain in DISPOSABLE_EMAIL_DOMAINS:
        return -10, f"email: disposable domain {domain} (-10)"
    if domain in FREE_EMAIL_DOMAINS:
        return 0, f"email: free domain {domain} (+0)"
    return 10, f"email: business domain {domain} (+10)"


def _score_contact(company: str, phone: str) -> tuple[int, List[str]]:
    points = 0
    notes = []
    if company and company.strip():
        points += 5
        notes.append("company provided (+5)")
    if phone and phone.strip():
        points += 8
        notes.append("phone provided (+8)")
    return points, notes


def _score_message(message: str) -> tuple[int, List[str]]:
    if not message or not message.strip():
        return 0, []
    points = 0
    notes = []
    msg = message.strip()
    if len(msg) > 100:
        points += 4
        notes.append(f"detailed message ({len(msg)} chars, +4)")
    lower = msg.lower()
    if _DATA_SIGNAL_PATTERN.search(lower):
        points += 3
        notes.append("data-signal keyword in message (+3)")
    if re.search(r"\d", msg):
        points += 3
        notes.append("numeric figures in message (+3)")
    return points, notes


def score_lead(lead: Dict[str, Any]) -> Dict[str, Any]:
    """
    Score a lead and return {score, tier, factors}.

    Args:
        lead: dict with keys: name, email, company, phone, service, message
              (any may be missing - scorer handles None/empty)
    """
    factors: List[str] = [f"base (+{BASE_SCORE})"]
    score = BASE_SCORE

    pts, note = _score_service(lead.get("service", ""))
    score += pts
    factors.append(note)

    pts, note = _score_email(lead.get("email", ""))
    score += pts
    factors.append(note)

    pts, notes = _score_contact(lead.get("company", ""), lead.get("phone", ""))
    score += pts
    factors.extend(notes)

    pts, notes = _score_message(lead.get("message", ""))
    score += pts
    factors.extend(notes)

    score = max(0, min(100, score))

    return {
        "score": score,
        "tier": _tier(score),
        "factors": factors,
    }
