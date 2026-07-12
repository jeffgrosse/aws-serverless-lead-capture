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


def test_dynamodb_write_failure_releases_key_and_returns_error(lambda_env, idempotency_table, monkeypatch):
    """
    Regression test for a bug found in code review: a failed DynamoDB write
    was caught and only logged, but idempotency.complete() still ran
    afterward, permanently caching a fake "Success" for every future retry
    with that key even though the lead was never recorded.

    The fix: on a write failure, release() the key instead (nothing was
    written, so a retry is safe) and return a non-200 response. Points
    LEADS_TABLE_NAME at a table that was never created via the `leads_table`
    fixture, so put_item raises a real ResourceNotFoundException from moto.
    """
    import lambda_function

    monkeypatch.setenv("LEADS_TABLE_NAME", "NonexistentLeadsTable")
    event = _event({"name": "Ada", "email": "ada@example.com"}, idempotency_key="write-fail-key")

    response = lambda_function.lambda_handler(event, None)

    assert response["statusCode"] != 200
    # release() deleted the reservation entirely - a retry can acquire it again.
    stored = idempotency_table.get_item(Key={"idempotency_key": "write-fail-key"}).get("Item")
    assert stored is None


def test_sns_publish_failure_leaves_key_in_progress_not_complete(lambda_env, leads_table, idempotency_table, monkeypatch):
    """
    Regression test for the same code-review bug as above, for the SNS side:
    a swallowed publish failure must not reach complete() either. Unlike the
    DynamoDB case, the key is NOT released here - the lead was already
    written, so releasing would let a retry re-run put_item and create a
    duplicate lead record. It's left IN_PROGRESS (the same documented
    crash-mid-processing tradeoff in docs/ARCHITECTURE.md) rather than
    falsely marked COMPLETE.
    """
    import lambda_function

    monkeypatch.setenv("LEAD_TOPIC_ARN", "arn:aws:sns:us-east-1:123456789012:NonexistentTopic")
    event = _event({"name": "Ada", "email": "ada@example.com"}, idempotency_key="sns-fail-key")

    response = lambda_function.lambda_handler(event, None)

    assert response["statusCode"] != 200
    assert leads_table.scan()["Count"] == 1  # the write itself genuinely succeeded

    stored = idempotency_table.get_item(Key={"idempotency_key": "sns-fail-key"})["Item"]
    assert stored["status"] == "IN_PROGRESS"  # never falsely marked COMPLETE


def test_complete_failure_after_successful_write_still_returns_success(lambda_env, leads_table, monkeypatch):
    """
    Regression test for the code-review finding that idempotency.complete()
    was the one AWS call not wrapped in try/except: if it fails after the
    DynamoDB write and SNS publish already succeeded, the caller who did
    everything right must still get a success response, not an unhandled 500.
    """
    import lambda_function

    def _raise(*args, **kwargs):
        raise RuntimeError("simulated complete() failure")

    monkeypatch.setattr(lambda_function.idempotency, "complete", _raise)
    event = _event({"name": "Ada", "email": "ada@example.com"}, idempotency_key="complete-fail-key")

    response = lambda_function.lambda_handler(event, None)

    assert response["statusCode"] == 200
    assert json.loads(response["body"])["message"] == "Success"
    assert leads_table.scan()["Count"] == 1  # the write genuinely happened


def test_non_object_body_returns_400_without_reserving_key(lambda_env, idempotency_table):
    """
    Regression test: a non-object JSON body (here a bare array) used to crash
    at body.get(...) *after* the idempotency key was already reserved,
    stranding it IN_PROGRESS for the full TTL. Validation now runs before
    reserve(), so malformed input gets a clean 400 and never touches the
    idempotency table at all.
    """
    import lambda_function

    event = {"headers": {"Idempotency-Key": "bad-body-key"}, "body": json.dumps([1, 2, 3])}

    response = lambda_function.lambda_handler(event, None)

    assert response["statusCode"] == 400
    assert idempotency_table.get_item(Key={"idempotency_key": "bad-body-key"}).get("Item") is None


def test_wrong_field_type_returns_400_without_reserving_key(lambda_env, idempotency_table):
    """
    Regression test: a dict body with a wrong-typed field (phone sent as a
    JSON number) used to crash inside scorer.py's phone.strip() call after
    the idempotency key was already reserved. Same fix as above.
    """
    import lambda_function

    event = _event(
        {"name": "Ada", "email": "ada@example.com", "phone": 15551234567},
        idempotency_key="bad-type-key",
    )

    response = lambda_function.lambda_handler(event, None)

    assert response["statusCode"] == 400
    assert idempotency_table.get_item(Key={"idempotency_key": "bad-type-key"}).get("Item") is None


def test_message_field_accepted_as_alias_for_note(lambda_env, leads_table):
    """
    Regression test: a caller who sends "message" (the field name documented
    in scorer.py's own score_lead() docstring) instead of "note" (this API's
    actual stored/notified field name) used to have that content silently
    discarded everywhere by `{**body, "message": note}`.
    """
    import lambda_function

    long_message = "We need API integration, budget $50k, roughly 200 users. " * 2
    event = _event({"name": "Ada", "email": "ada@example.com", "message": long_message})

    response = lambda_function.lambda_handler(event, None)

    assert response["statusCode"] == 200
    stored = leads_table.scan()["Items"][0]
    assert stored["note"] == long_message
    assert any("data-signal keyword" in f for f in stored["factors"])


def test_response_has_no_cors_headers_relies_on_httpapi_config(lambda_env, leads_table):
    """
    Regression test: CORS headers must NOT be set in the Lambda response.
    template.yaml's HttpApi CorsConfiguration is the single source of truth;
    duplicating headers here creates drift when the README's own production
    advice (restrict AllowOrigins) is followed.
    """
    import lambda_function

    response = lambda_function.lambda_handler(
        _event({"name": "x", "email": "x@example.com"}), None
    )

    assert "Access-Control-Allow-Origin" not in response.get("headers", {})
    assert "Access-Control-Allow-Headers" not in response.get("headers", {})
