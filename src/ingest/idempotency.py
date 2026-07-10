"""
DynamoDB-backed idempotency for the ingest Lambda.

Design (see docs/ARCHITECTURE.md for the full rationale):
  - The caller supplies an `Idempotency-Key` header (any stable string - a
    UUID, a form-session ID, a content hash it computed itself). If absent,
    resolve_key() falls back to a SHA256 hash of the raw request body, so
    exact-duplicate submissions from callers that don't send the header
    still get deduplicated.
  - reserve() does a conditional PutItem BEFORE any side effect (DynamoDB
    lead write, SNS publish) runs. This is what closes the race window for
    two near-simultaneous duplicate requests: DynamoDB's conditional write
    is atomic, so of two concurrent reserve() calls for the same key,
    exactly one succeeds. Reserving only after processing would let both
    requests through and both fan out before either write landed.
  - complete() stores the final response body against the key so a retry
    that arrives after the original request finished can replay the exact
    same response instead of re-running side effects.
  - Records TTL out via `expires_at` (handled by the DynamoDB table's TTL
    spec in template.yaml), so old keys never need a manual cleanup job.
"""

import hashlib
import time
from dataclasses import dataclass
from typing import Any, Optional

import boto3
from botocore.exceptions import ClientError

STATUS_IN_PROGRESS = "IN_PROGRESS"
STATUS_COMPLETE = "COMPLETE"


@dataclass
class Reservation:
    # True if this call reserved the key and should proceed to process the
    # request. False if the key was already reserved by a prior request.
    acquired: bool
    # Set only when acquired is False: the stored response to replay, or
    # None if the original request is still IN_PROGRESS (a genuinely
    # concurrent duplicate, not a retry-after-completion).
    existing_response: Optional[dict] = None


def resolve_key(headers: dict, raw_body: str) -> str:
    """Idempotency-Key header (case-insensitive) if present, else sha256(body)."""
    for name, value in (headers or {}).items():
        if name.lower() == "idempotency-key" and value:
            return value
    return hashlib.sha256((raw_body or "").encode("utf-8")).hexdigest()


class IdempotencyStore:
    def __init__(self, table_name: str, ttl_hours: float):
        self._table = boto3.resource("dynamodb").Table(table_name)
        self._ttl_seconds = int(ttl_hours * 3600)

    def reserve(self, key: str) -> Reservation:
        expires_at = int(time.time()) + self._ttl_seconds
        try:
            self._table.put_item(
                Item={
                    "idempotency_key": key,
                    "status": STATUS_IN_PROGRESS,
                    "expires_at": expires_at,
                },
                ConditionExpression="attribute_not_exists(idempotency_key)",
            )
            return Reservation(acquired=True)
        except ClientError as e:
            if e.response["Error"]["Code"] != "ConditionalCheckFailedException":
                raise

        existing = self._table.get_item(Key={"idempotency_key": key}).get("Item")
        if existing and existing.get("status") == STATUS_COMPLETE:
            return Reservation(acquired=False, existing_response=existing.get("response"))
        # Either the record was never found (e.g. it expired between the
        # failed PutItem and this GetItem) or it's still IN_PROGRESS.
        return Reservation(acquired=False, existing_response=None)

    def complete(self, key: str, response: dict) -> None:
        self._table.update_item(
            Key={"idempotency_key": key},
            UpdateExpression="SET #s = :complete, #r = :response",
            ExpressionAttributeNames={"#s": "status", "#r": "response"},
            ExpressionAttributeValues={":complete": STATUS_COMPLETE, ":response": response},
        )
