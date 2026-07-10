"""
Unit tests for src/ingest/idempotency.py: resolve_key() (a pure function) and
IdempotencyStore (reserve/complete against a moto-mocked DynamoDB table).

End-to-end duplicate-submission behavior through the Lambda handler lives in
tests/test_lambda_function.py.
"""

import hashlib

from idempotency import IdempotencyStore, Reservation, resolve_key


# --- resolve_key --------------------------------------------------------------

def test_resolve_key_prefers_header():
    headers = {"Idempotency-Key": "client-supplied-key"}
    assert resolve_key(headers, '{"email": "a@example.com"}') == "client-supplied-key"


def test_resolve_key_header_is_case_insensitive():
    headers = {"IDEMPOTENCY-KEY": "client-supplied-key"}
    assert resolve_key(headers, "{}") == "client-supplied-key"


def test_resolve_key_falls_back_to_body_hash_when_header_absent():
    body = '{"email": "a@example.com"}'
    expected = hashlib.sha256(body.encode("utf-8")).hexdigest()
    assert resolve_key({}, body) == expected
    # Same body -> same key, so exact-duplicate JSON submissions still dedup.
    assert resolve_key({}, body) == resolve_key({}, body)


def test_resolve_key_falls_back_on_non_json_body():
    """resolve_key() hashes the raw body bytes without parsing them, so a
    non-JSON payload (or an empty body) still produces a deterministic key
    instead of raising or falling through to an unstable value."""
    non_json = "plain text, not json"
    assert resolve_key({}, non_json) == resolve_key({}, non_json)
    assert resolve_key({}, "") == hashlib.sha256(b"").hexdigest()


# --- IdempotencyStore.reserve / complete ---------------------------------------

def test_reserve_new_key_acquires(idempotency_table):
    store = IdempotencyStore(table_name=idempotency_table.name, ttl_hours=24)
    assert store.reserve("new-key") == Reservation(acquired=True)


def test_reserve_duplicate_key_still_in_progress_denies_without_response(idempotency_table):
    store = IdempotencyStore(table_name=idempotency_table.name, ttl_hours=24)
    store.reserve("in-flight-key")

    reservation = store.reserve("in-flight-key")

    assert reservation.acquired is False
    assert reservation.existing_response is None


def test_reserve_duplicate_key_after_complete_returns_stored_response(idempotency_table):
    store = IdempotencyStore(table_name=idempotency_table.name, ttl_hours=24)
    store.reserve("finished-key")
    store.complete("finished-key", {"message": "Success", "score": 42})

    reservation = store.reserve("finished-key")

    assert reservation.acquired is False
    assert reservation.existing_response == {"message": "Success", "score": 42}


def test_reserve_distinct_keys_both_acquire(idempotency_table):
    store = IdempotencyStore(table_name=idempotency_table.name, ttl_hours=24)

    assert store.reserve("key-a").acquired is True
    assert store.reserve("key-b").acquired is True


def test_complete_regression_response_is_dynamodb_reserved_word(idempotency_table):
    """
    Regression test for a bug found while validating this module with moto:
    complete()'s UpdateExpression originally wrote `SET response = :response`,
    but `response` is a DynamoDB reserved word, so every call raised
    `ValidationException: ... reserved keyword`. The fix aliases it via
    ExpressionAttributeNames (`#r` -> `response`).

    To verify this test actually exercises the regression: temporarily change
    complete() to use the bare word `response` in UpdateExpression instead of
    `#r`, and drop "#r" from ExpressionAttributeNames - this test should then
    fail with ValidationException.
    """
    store = IdempotencyStore(table_name=idempotency_table.name, ttl_hours=24)
    store.reserve("reserved-word-key")

    store.complete("reserved-word-key", {"message": "Success"})  # must not raise

    stored = idempotency_table.get_item(Key={"idempotency_key": "reserved-word-key"})["Item"]
    assert stored["response"] == {"message": "Success"}
    assert stored["status"] == "COMPLETE"
