"""
Sends the same submission with the same Idempotency-Key twice against a
deployed stack and confirms: both responses are identical 200s, and exactly
one lead record was written (no duplicate fan-out).

Usage:
    python3 verify_idempotency.py --stack-name aws-serverless-lead-capture
"""

import argparse
import json
import time
import urllib.error
import urllib.request
import uuid

import boto3


def stack_outputs(stack_name: str, region: str) -> dict:
    cfn = boto3.client("cloudformation", region_name=region)
    stack = cfn.describe_stacks(StackName=stack_name)["Stacks"][0]
    return {o["OutputKey"]: o["OutputValue"] for o in stack["Outputs"]}


def post(endpoint: str, payload: dict, idempotency_key: str) -> tuple[int, str]:
    req = urllib.request.Request(
        endpoint,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json", "Idempotency-Key": idempotency_key},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.status, resp.read().decode("utf-8")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8")


def count_leads_with_email(table_name: str, region: str, email: str) -> int:
    table = boto3.resource("dynamodb", region_name=region).Table(table_name)
    count = 0
    scan_kwargs = {"FilterExpression": boto3.dynamodb.conditions.Attr("email").eq(email)}
    while True:
        resp = table.scan(**scan_kwargs)
        count += len(resp.get("Items", []))
        if "LastEvaluatedKey" not in resp:
            return count
        scan_kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stack-name", default="aws-serverless-lead-capture")
    parser.add_argument("--region", default=boto3.Session().region_name or "us-east-1")
    args = parser.parse_args()

    outputs = stack_outputs(args.stack_name, args.region)
    endpoint = outputs["LeadApiEndpoint"]
    leads_table = outputs["LeadsTableName"]

    test_email = f"verify-idempotency-{int(time.time())}@example.com"
    idempotency_key = str(uuid.uuid4())
    payload = {
        "name": "Idempotency Verification",
        "email": test_email,
        "company": "Test Co",
        "service": "standard plan",
        "note": "Automated idempotency check.",
    }

    status_a, body_a = post(endpoint, payload, idempotency_key)
    status_b, body_b = post(endpoint, payload, idempotency_key)

    print(f"First request:  {status_a} {body_a}")
    print(f"Second request: {status_b} {body_b}")

    assert status_a == 200, f"first request did not return 200: {status_a}"
    assert status_b == 200, f"second request did not return 200: {status_b}"
    assert body_a == body_b, "duplicate request did not replay the original response"

    # DynamoDB writes are eventually visible to a Scan almost immediately in
    # practice, but give it a moment before checking.
    time.sleep(2)
    lead_count = count_leads_with_email(leads_table, args.region, test_email)
    print(f"Lead records written for {test_email}: {lead_count}")
    assert lead_count == 1, f"expected exactly 1 lead record, found {lead_count}"

    print("PASS: duplicate submission was deduplicated.")


if __name__ == "__main__":
    main()
