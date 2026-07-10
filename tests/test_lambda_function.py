"""
End-to-end tests for the ingest Lambda handler (src/ingest/lambda_function.py)
against a moto-mocked LeadsTable, IdempotencyTable, and SNS topic.

Unit tests for the idempotency layer itself live in tests/test_idempotency.py.
"""

import json
from decimal import Decimal


def _event(body: dict, idempotency_key=None) -> dict:
    headers = {"Content-Type": "application/json"}
    if idempotency_key is not None:
        headers["Idempotency-Key"] = idempotency_key
    return {"headers": headers, "body": json.dumps(body)}


def test_duplicate_submission_same_key_returns_identical_response(lambda_env, leads_table):
    import lambda_function

    event = _event({"name": "Ada", "email": "ada@example.com"}, idempotency_key="same-key")

    first = lambda_function.lambda_handler(event, None)
    second = lambda_function.lambda_handler(event, None)

    assert first["statusCode"] == 200
    assert second["statusCode"] == 200
    assert first["body"] == second["body"]
    assert leads_table.scan()["Count"] == 1


def test_distinct_idempotency_keys_both_process(lambda_env, leads_table):
    import lambda_function

    lambda_function.lambda_handler(
        _event({"name": "Ada", "email": "ada@example.com"}, idempotency_key="key-a"), None
    )
    lambda_function.lambda_handler(
        _event({"name": "Bob", "email": "bob@example.com"}, idempotency_key="key-b"), None
    )

    assert leads_table.scan()["Count"] == 2


def test_missing_header_falls_back_to_hash_dedup(lambda_env, leads_table):
    import lambda_function

    event = _event({"name": "Ada", "email": "ada@example.com"})  # no Idempotency-Key

    lambda_function.lambda_handler(event, None)
    lambda_function.lambda_handler(event, None)

    assert leads_table.scan()["Count"] == 1


def test_missing_header_different_bodies_both_process(lambda_env, leads_table):
    import lambda_function

    lambda_function.lambda_handler(_event({"name": "Ada", "email": "ada@example.com"}), None)
    lambda_function.lambda_handler(_event({"name": "Bob", "email": "bob@example.com"}), None)

    assert leads_table.scan()["Count"] == 2


def test_replayed_response_with_decimal_score_serializes_correctly(lambda_env, idempotency_table):
    """
    Regression test for a bug found while validating this module with moto:
    a completed response is stored in DynamoDB, which returns numeric
    attributes as Decimal rather than int/float on read-back. When a
    duplicate request replayed that stored response, json.dumps(...) raised
    `TypeError: Object of type Decimal is not JSON serializable`, since
    json.dumps has no default encoder for Decimal. The fix passes
    `default=_json_default` in _respond(), which coerces whole-number
    Decimals back to int.

    To verify this test actually exercises the regression: temporarily call
    `json.dumps(payload)` in _respond() without `default=_json_default` -
    this test should then fail with that TypeError.
    """
    import lambda_function

    event = _event(
        {"name": "Ada", "email": "ada@example.com", "service": "enterprise plan"},
        idempotency_key="decimal-key",
    )

    first = lambda_function.lambda_handler(event, None)
    first_body = json.loads(first["body"])
    assert isinstance(first_body["score"], int)

    # Confirm DynamoDB actually round-tripped the stored score as a Decimal,
    # i.e. that this test exercises the code path the bug lived in.
    stored = idempotency_table.get_item(Key={"idempotency_key": "decimal-key"})["Item"]
    assert isinstance(stored["response"]["score"], Decimal)

    second = lambda_function.lambda_handler(event, None)  # replays the stored response
    second_body = json.loads(second["body"])

    assert second_body == first_body
    assert isinstance(second_body["score"], int)
