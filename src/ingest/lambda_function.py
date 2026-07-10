import json
import os
import uuid
from datetime import datetime, timezone
from decimal import Decimal

import boto3

from idempotency import IdempotencyStore, resolve_key
from scorer import score_lead

CORS_HEADERS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Headers": "Content-Type, Idempotency-Key",
}

dynamodb = boto3.resource("dynamodb")
sns = boto3.client("sns")
idempotency = IdempotencyStore(
    table_name=os.environ["IDEMPOTENCY_TABLE_NAME"],
    ttl_hours=float(os.environ.get("IDEMPOTENCY_TTL_HOURS", "24")),
)


def _json_default(obj):
    # A replayed response was round-tripped through DynamoDB, which returns
    # numbers as Decimal rather than int/float.
    if isinstance(obj, Decimal):
        return int(obj) if obj % 1 == 0 else float(obj)
    raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")


def _respond(status_code: int, payload: dict) -> dict:
    return {
        "statusCode": status_code,
        "headers": CORS_HEADERS,
        "body": json.dumps(payload, default=_json_default),
    }


def lambda_handler(event, context):
    raw_body = event.get("body", "{}")
    try:
        body = json.loads(raw_body)
    except (TypeError, ValueError):
        body = {}

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
    note = body.get("note", "")
    scoring = score_lead({**body, "message": note})

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
        print("DynamoDB error:", str(e))

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
        print("SNS error:", str(e))

    response_payload = {"message": "Success", "score": scoring["score"], "tier": scoring["tier"]}
    idempotency.complete(key, response_payload)
    return _respond(200, response_payload)
