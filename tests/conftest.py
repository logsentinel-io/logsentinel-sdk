import os
from datetime import datetime, timezone

import boto3
import pytest
from moto import mock_aws

from logsentinel_sdk import generate_sentinel_id


@pytest.fixture
def aws_credentials():
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-1"

@pytest.fixture
def aws_setup(aws_credentials):
    with mock_aws():
        kinesis_client = boto3.client("kinesis")
        kinesis_client.create_stream(StreamName="logsentinel-stream", ShardCount=1)

        sqs_client = boto3.client("sqs")
        response = sqs_client.create_queue(QueueName="logsentinel-dlq")

        ssm_client = boto3.client("ssm")
        ssm_client.put_parameter(
            Name="/logsentinel/stream-name",
            Value="logsentinel-stream",
            Type="String"
            )
        ssm_client.put_parameter(
            Name="/logsentinel/dlq-url",
            Value=response["QueueUrl"],
            Type="String"
        )
        yield kinesis_client, ssm_client, sqs_client

@pytest.fixture
def records_test():
    test_sentinel_id = generate_sentinel_id()

    test_records = [
        {
            "sentinel_id": test_sentinel_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": "INFO",
            "message": "Premier enregistrement de test : initialisation",
            "service": "test-service",
            "metadata": {"user_id": 123, "action": "login"}
        },
        {
            "sentinel_id": test_sentinel_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": "WARNING",
            "message": "Deuxième enregistrement de test : latence détectée",
            "service": "test-service",
            "metadata": {"latency_ms": 450, "threshold": 400}
        },
        {
            "sentinel_id": test_sentinel_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": "ERROR",
            "message": "Troisième enregistrement de test : échec de connexion",
            "service": "test-service",
            "metadata": {"error_code": "ECONNREFUSED", "retries": 3}
        }
    ]
    return test_records

@pytest.fixture
def failed_records():
    return [{
            "FailedRecordCount": 3,
            "Records": [
                {
                    "ErrorCode": "ProvisionedThroughputExceededException",
                    "ErrorMessage": "...",
                },
                {
                    "ErrorCode": "ProvisionedThroughputExceededException",
                    "ErrorMessage": "...",
                },
                {
                    "ErrorCode": "ProvisionedThroughputExceededException",
                    "ErrorMessage": "...",
                },
            ],
        },
        {
            "FailedRecordCount": 3,
            "Records": [
                {
                    "ErrorCode": "ProvisionedThroughputExceededException",
                    "ErrorMessage": "...",
                },
                {
                    "ErrorCode": "ProvisionedThroughputExceededException",
                    "ErrorMessage": "...",
                },
                {
                    "ErrorCode": "ProvisionedThroughputExceededException",
                    "ErrorMessage": "...",
                },
            ],
        },
        {
            "FailedRecordCount": 3,
            "Records": [
                {
                    "ErrorCode": "ProvisionedThroughputExceededException",
                    "ErrorMessage": "...",
                },
                {
                    "ErrorCode": "ProvisionedThroughputExceededException",
                    "ErrorMessage": "...",
                },
                {
                    "ErrorCode": "ProvisionedThroughputExceededException",
                    "ErrorMessage": "...",
                },
            ],
        },
        {
            "FailedRecordCount": 3,
            "Records": [
                {
                    "ErrorCode": "ProvisionedThroughputExceededException",
                    "ErrorMessage": "...",
                },
                {
                    "ErrorCode": "ProvisionedThroughputExceededException",
                    "ErrorMessage": "...",
                },
                {
                    "ErrorCode": "ProvisionedThroughputExceededException",
                    "ErrorMessage": "...",
                },
            ],
        },
        {
            "FailedRecordCount": 3,
            "Records": [
                {
                    "ErrorCode": "ProvisionedThroughputExceededException",
                    "ErrorMessage": "...",
                },
                {
                    "ErrorCode": "ProvisionedThroughputExceededException",
                    "ErrorMessage": "...",
                },
                {
                    "ErrorCode": "ProvisionedThroughputExceededException",
                    "ErrorMessage": "...",
                },
            ],
        }]