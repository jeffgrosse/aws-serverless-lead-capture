"""
Unit tests for src/ingest/scorer.py's scoring helpers.
"""

from scorer import score_lead


def test_malformed_email_empty_domain_scores_as_malformed_not_business():
    """
    Regression test: an email like "test@" (an "@" with nothing after it)
    passed _score_email's guard and fell through to the "business domain"
    branch with an empty domain string, scoring identically to a verified
    corporate email.
    """
    result = score_lead({"email": "test@"})
    assert not any("business domain" in f for f in result["factors"])
    assert any("missing or malformed" in f for f in result["factors"])


def test_data_signal_keyword_requires_word_boundary():
    """
    Regression test: DATA_SIGNAL_KEYWORDS matching used naive substring
    containment, so short keywords like "api" false-positive-matched inside
    unrelated words ("rapidly", "capital").
    """
    false_positive = score_lead({"message": "We need this delivered rapidly, based in the capital."})
    assert not any("data-signal keyword" in f for f in false_positive["factors"])

    true_positive = score_lead({"message": "We need API integration for our platform."})
    assert any("data-signal keyword" in f for f in true_positive["factors"])
