import json
import os
import sys
import time
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from mypy_boto3_kinesis.type_defs import PutRecordsRequestEntryTypeDef

class KinesisClient:
    """Low-level Kinesis writer with exponential-backoff retry and SQS DLQ fallback.

    On persistent failure (all retries exhausted), records are:

    1. Printed as JSON to stdout so they land in CloudWatch Logs.
    2. Sent individually to the SQS Dead Letter Queue for later replay.

    Retry delays (default): 100 ms → 200 ms → 400 ms → 800 ms → 1 600 ms.
    Override via env vars ``LOGSENTINEL_RETRY_BASE_MS`` and ``LOGSENTINEL_MAX_RETRIES``.
    """

    def __init__\
    (self, kinesis_client: Any, sqs_client: Any, stream_name: str, dlq_url: str)\
    -> None:
        """Initialise the client with pre-built boto3 clients.

        Args:
            kinesis_client: A boto3 Kinesis client (``boto3.client("kinesis")``).
            sqs_client: A boto3 SQS client (``boto3.client("sqs")``).
            stream_name: Name of the target Kinesis Data Stream
                (read from SSM by :class:`Logger`).
            dlq_url: URL of the SQS Dead Letter Queue for failed records
                (read from SSM by :class:`Logger`).
        """
        self.kinesis_client = kinesis_client
        self.sqs_client = sqs_client
        self.stream_name = stream_name
        self.dlq_url = dlq_url
        try:
            self._base_ms = int(os.environ.get("LOGSENTINEL_RETRY_BASE_MS", "100"))
        except ValueError:
            self._base_ms = 100
            print("Wrong value type, the system will use default one", file=sys.stderr)

        try:
            self._max_retries = int(os.environ.get("LOGSENTINEL_MAX_RETRIES", "5"))
        except ValueError:
            self._max_retries = 5
            print("Wrong value type, the system will use default one", file=sys.stderr)


    def _find_failed(self,response_records: dict[str, Any]) -> list[int]:
        failed_indices = [
            index for index, record in enumerate(response_records["Records"])
            if "ErrorCode" in record
        ]
        return failed_indices

    def flush(self, records: list[dict[str, Any]]) -> None:
        """Send records to Kinesis, retrying failed entries with exponential backoff.

        All records share the same partition key (the ``sentinel_id`` of the
        first record), so all events from one execution land on the same shard
        in order.

        Args:
            records: List of log record dicts as built by :class:`Logger`.
                Must be non-empty. Each dict must contain a ``"sentinel_id"`` key.
        """
        sentinel_id = records[0]["sentinel_id"]
        formated_records: list[PutRecordsRequestEntryTypeDef] = [
            {
                "Data": json.dumps(record).encode("utf-8"),
                "PartitionKey": sentinel_id
            }
        for record in records]
        records_to_send = formated_records
        original_records = records
        for attempt in range(self._max_retries):
            kinesis_response = self.kinesis_client.put_records(
                StreamName= self.stream_name,
                Records=records_to_send,
            )
            failed_records = self._find_failed(kinesis_response)
            if len(failed_records) == 0:
                return None
            records_to_send = [records_to_send[i] for i in failed_records]
            original_records = [original_records[i] for i in failed_records]
            if attempt != self._max_retries - 1:
                time.sleep(self._base_ms / 1000 * (2 ** attempt))
        [print(json.dumps(original_record)) for original_record in original_records]
        try:
            for record in original_records:
                self.sqs_client.send_message(
                    QueueUrl=self.dlq_url,
                    MessageBody=json.dumps(record),
                )
        except Exception as error:
            print("Failed to send message into sqs" + str(error), file=sys.stderr)

