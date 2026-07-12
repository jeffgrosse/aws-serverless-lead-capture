import json
import os
import uuid
from datetime import datetime, timezone
from decimal import Decimal

import boto3

from idempotency import IdempotencyStore, resolve_key
from scorer import score_lead

dynamodb = boto3.resource("dynamodb")
sns = boto3.client("sns")
idempotency = IdempotencyStore(
    table_name=os.environ["IDEMPOTENCY_TABLE_NAME"],
    ttl_hours=float(os.environ.get("IDEMPOTENCY_TTL_HOURS", "24")),
)

_STRING_FIELDS = ("name", "email", "company", "phone", "service", "note")


def _validate_body(body) -> str:
    """Returns an error message if `body` isn't a usable request shape, else ""."""
    if not isinstance(body, dict):
        return "request body must be a JSON object"
    for field in _STRING_FIELDS:
        if field in body and body[field] is not None and not isinstance(body[field], str):
            return f"'{field}' must be a string"
    return ""


def _json_default(obj):
    # A replayed response was round-tripped through DynamoDB, which returns
    # numbers as Decimal rather than int/float.
    if isinstance(obj, Decimal):
        return int(obj) if obj % 1 == 0 else float(obj)
    raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")


def _respond(status_code: int, payload: dict) -> dict:
    # No CORS headers here on purpose - template.yaml's HttpApi
    # CorsConfiguration is the single source of truth (AWS HTTP APIs apply
    # configured CORS headers to every response automatically, not just
    # preflight OPTIONS). Duplicating them here risked drifting out of sync,
    # e.g. if AllowOrigins is restricted for production per the README but
    # this dict is left at "*".
    return {
        "statusCode": status_code,
        "body": json.dumps(payload, default=_json_default),
    }


def lambda_handler(event, context):
    raw_body = event.get("body", "{}")
    try:
        body = json.loads(raw_body)
    except (TypeError, ValueError):
        body = {}

    # Validate BEFORE reserving an idempotency key - a malformed body must
    # never reserve a key it then can't complete, which would strand that
    # key IN_PROGRESS for the full TTL with no way to retry cleanly.
    validation_error = _validate_body(body)
    if validation_error:
        return _respond(400, {"message": validation_error})

    key = resolve_key(event.get("headers", {}), raw_body)

    # Reserve the key BEFORE any side effect runs. A failure here must fail
    # the request rather than silently continue (unlike the DynamoDB/SNS
    # calls below) - if we can't establish the dedup record, we can't
    # guarantee the request won't be double-processed.
    reservation = idempotency.reserve(key)
    if not reservation.acquired:
        if reservation.existing_response is not None:
            # Retry-after-completion: replay the original response verbatim.
            return _respond(200, reservation.existing_response)
        # Genuinely concurrent duplicate still in flight. Ack without
        # re-running side effects rather than blocking/polling for the
        # original request to finish - see docs/ARCHITECTURE.md.
        return _respond(200, {"message": "Accepted (duplicate request in progress)"})

    name = body.get("name", "Unknown")
    email = body.get("email", "Unknown")
    company = body.get("company", "")
    phone = body.get("phone", "")
    service = body.get("service", "")
    # "message" is accepted as an alias for "note" - scorer.py's own
    # score_lead() docstring documents the field as "message", but this
    # API's real, stored/notified field is "note". A caller who followed the
    # scorer docstring instead of the README used to have that content
    # silently discarded everywhere (storage, SNS text, and scoring) by
    # `{**body, "message": note}`, which always overwrote "message" with the
    # (usually empty) "note" default.
    note = body.get("note") or body.get("message") or ""
    scoring = score_lead({
        "email": email,
        "company": company,
        "phone": phone,
        "service": service,
        "message": note,
    })

    try:
        table = dynamodb.Table(os.environ["LEADS_TABLE_NAME"])
        table.put_item(Item={
            "id": str(uuid.uuid4()),
            "name": name,
            "email": email,
            "company": company,
            "phone": phone,
            "service": service,
            "note": note,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "score": scoring["score"],
            "tier": scoring["tier"],
            "factors": scoring["factors"],
        })
    except Exception as e:
        # Nothing was written, so it's safe to release the key - a retry can
        # attempt the whole flow again with no risk of a duplicate lead. Must
        # NOT fall through to complete(): that would permanently cache this
        # failure as a fake "Success" for every future retry (see
        # docs/ARCHITECTURE.md - the idempotency table exists specifically to
        # avoid caching side effects that never actually happened).
        print("DynamoDB error:", str(e))
        idempotency.release(key)
        return _respond(502, {"message": "Failed to record lead - please retry"})

    try:
        sns.publish(
            TopicArn=os.environ["LEAD_TOPIC_ARN"],
            Subject=f"[{scoring['tier']}-{scoring['score']}] New Lead: {name} from {company}",
            Message=(
                f"Score: {scoring['score']}/100  (Tier {scoring['tier']})\n\n"
                f"Name:    {name}\n"
                f"Email:   {email}\n"
                f"Company: {company}\n"
                f"Phone:   {phone}\n"
                f"Service: {service}\n\n"
                f"Message:\n{note or '(none)'}\n\n"
                f"Why this score:\n" + "\n".join(f"  - {fct}" for fct in scoring["factors"])
            ),
        )
    except Exception as e:
        # The lead is already in LeadsTable, so this must NOT release() the
        # key - a retry would re-run put_item with a fresh uuid and create a
        # duplicate lead record, which is worse than a missed notification.
        # Also must NOT call complete(): this attempt didn't fully succeed,
        # so it shouldn't be cached as one. The key is left IN_PROGRESS,
        # which is the same documented crash-mid-processing tradeoff
        # docs/ARCHITECTURE.md already calls out - a retry within the TTL
        # gets an ack without reprocessing, a retry after TTL gets a fresh
        # attempt (which will re-send the notification).
        print("SNS error:", str(e))
        return _respond(502, {"message": "Lead recorded, but the notification failed"})

    response_payload = {"message": "Success", "score": scoring["score"], "tier": scoring["tier"]}
    try:
        idempotency.complete(key, response_payload)
    except Exception as e:
        # The write and the publish both already succeeded - don't let a
        # failure in bookkeeping the response mask that real success. Worst
        # case the key stays IN_PROGRESS until TTL (same documented tradeoff
        # as above), but the caller who actually did everything right still
        # gets told so.
        print("Idempotency complete() error:", str(e))
    return _respond(200, response_payload)
