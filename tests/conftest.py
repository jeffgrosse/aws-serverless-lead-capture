"""Shared pytest fixtures: moto-mocked DynamoDB/SNS and env vars lambda_function.py needs."""

import sys
from pathlib import Path

import boto3
import pytest
from moto import mock_aws

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src" / "ingest"))

REGION = "us-east-1"


@pytest.fixture(autouse=True)
def aws_credentials(monkeypatch):
    """Fake credentials so boto3 never attempts a real AWS call, even before moto is active."""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_SECURITY_TOKEN", "testing")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", REGION)


@pytest.fixture
def aws_mock():
    with mock_aws():
        yield


@pytest.fixture
def dynamodb(aws_mock):
    return boto3.resource("dynamodb", region_name=REGION)


@pytest.fixture
def leads_table(dynamodb):
    return dynamodb.create_table(
        TableName="LeadsTable",
        KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )


@pytest.fixture
def idempotency_table(dynamodb):
    return dynamodb.create_table(
        TableName="IdempotencyTable",
        KeySchema=[{"AttributeName": "idempotency_key", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "idempotency_key", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )


@pytest.fixture
def sns_topic(aws_mock):
    sns = boto3.client("sns", region_name=REGION)
    return sns.create_topic(Name="LeadsTopic")["TopicArn"]


@pytest.fixture
def lambda_env(monkeypatch, leads_table, idempotency_table, sns_topic):
    """Env vars lambda_function.py reads, backed by tables/topic already created above.

    IDEMPOTENCY_TABLE_NAME is read once at `import lambda_function` time; the rest are
    read fresh inside lambda_handler() on every call, so only the table name needs to
    stay constant across tests (it does - always "IdempotencyTable").
    """
    monkeypatch.setenv("LEADS_TABLE_NAME", leads_table.name)
    monkeypatch.setenv("IDEMPOTENCY_TABLE_NAME", idempotency_table.name)
    monkeypatch.setenv("LEAD_TOPIC_ARN", sns_topic)
    monkeypatch.setenv("IDEMPOTENCY_TTL_HOURS", "24")
