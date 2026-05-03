import os
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Self

import boto3

from logsentinel_sdk import KinesisClient

if TYPE_CHECKING:
    pass


class Logger:
    def __init__(
            self,
            service: str,
            sentinel_id: str,
            parent_service: str | None = None,
            endpoint_url: str | None = None,
    ) -> None:
        ssm_client = boto3.client("ssm", endpoint_url=endpoint_url)
        stream_name = ssm_client.get_parameter(Name="/logsentinel/stream-name")
        dlq_url = ssm_client.get_parameter(Name="/logsentinel/dlq-url")
        kinesis_client = boto3.client("kinesis", endpoint_url=endpoint_url)
        sqs_client = boto3.client("sqs", endpoint_url=endpoint_url)
        self._stream_name = stream_name["Parameter"]["Value"]
        self._lambda_request_id = os.environ.get("AWS_LAMBDA_REQUEST_ID")
        self._service = service
        self._sentinel_id = sentinel_id
        self._parent_service = parent_service
        self._buffer: list[dict[str, Any]] = []
        self._kinesis_client = KinesisClient(
            kinesis_client,
            sqs_client,
            self._stream_name,
            dlq_url["Parameter"]["Value"],
        )


    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: object) -> None:
        self.flush()

    def _log(self, level: str, message: str, **metadata: object) -> None:
        record: dict[str, Any] = {
            "sentinel_id" : self._sentinel_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": level,
            "message": message,
            "service": self._service,
        }
        if self._parent_service is not None:
            record["parent_service"] = self._parent_service
        if self._lambda_request_id is not None:
            record["lambda_request_id"] = self._lambda_request_id
        record["metadata"] = metadata
        self._buffer.append(record)


    def debug(self, message: str, **metadata: object) -> None:
        self._log("DEBUG", message, **metadata)

    def info(self, message: str, **metadata: object) -> None:
        self._log("INFO", message, **metadata)

    def warning(self, message: str, **metadata: object) -> None:
        self._log("WARNING", message, **metadata)

    def error(self, message: str, **metadata: object) -> None:
        self._log("ERROR", message, **metadata)

    def critical(self, message: str, **metadata: object) -> None:
        self._log("CRITICAL", message, **metadata)

    def flush(self) -> None:
        if len(self._buffer) == 0:
            return None
        self._kinesis_client.flush(self._buffer)
        self._buffer.clear()
        return None