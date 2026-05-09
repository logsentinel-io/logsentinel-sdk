# logsentinel-sdk

Python SDK for structured, correlated logging from AWS Lambda functions.

Generates a `sentinel_id` correlation ID at the execution entry point and propagates it through every downstream Lambda call, so all events from a single workflow can be retrieved as a unified timeline — without manually correlating CloudWatch log groups.

---

## Installation

```bash
pip install logsentinel-sdk
```

Requires Python 3.13+.

---

## Quick start

```python
from logsentinel_sdk import Logger, generate_sentinel_id

def handler(event, context):
    sentinel_id = event.get("sentinel_id") or generate_sentinel_id()
    with Logger(service="battle-service", sentinel_id=sentinel_id) as logger:
        logger.info("Battle started", pokemon="Pikachu", opponent="Mewtwo")
        # pass sentinel_id to every downstream call:
        return {"sentinel_id": sentinel_id}
```

The `with` block guarantees `flush()` is called even if the handler raises an exception.

---

## Propagation patterns

Different Lambda trigger types expose the incoming payload differently.

### Direct invoke / API Gateway (entry point)

Generate a new `sentinel_id` if none is present in the event.

```python
def handler(event, context):
    sentinel_id = event.get("sentinel_id") or generate_sentinel_id()
    with Logger(service="entry-service", sentinel_id=sentinel_id) as logger:
        ...
        return {"sentinel_id": sentinel_id, ...}
```

### Step Functions

Step Functions passes the full state input as the event dict.

```python
def handler(event, context):
    # sentinel_id was set by the entry Lambda and passed through the state machine input
    sentinel_id = event["sentinel_id"]
    with Logger(service="step-service", sentinel_id=sentinel_id) as logger:
        ...
```

### SQS trigger

Each record's body is a JSON string. Parse it to access the sentinel_id.

```python
import json

def handler(event, context):
    for record in event["Records"]:
        body = json.loads(record["body"])
        sentinel_id = body["sentinel_id"]
        with Logger(service="worker", sentinel_id=sentinel_id) as logger:
            ...
```

### EventBridge trigger

The sentinel_id lives inside `event["detail"]`.

```python
def handler(event, context):
    sentinel_id = event["detail"]["sentinel_id"]
    with Logger(service="notifier", sentinel_id=sentinel_id) as logger:
        ...
```

### Kinesis trigger (fanout)

Each shard record's data is base64-encoded. Decode and parse to extract the sentinel_id.

```python
import base64, json

def handler(event, context):
    for record in event["Records"]:
        payload = json.loads(base64.b64decode(record["kinesis"]["data"]))
        sentinel_id = payload["sentinel_id"]
        with Logger(service="processor", sentinel_id=sentinel_id) as logger:
            ...
```

### Nested service (parent tracking)

When a Lambda is called by another LogSentinel-instrumented service, pass `parent_service` to preserve the call graph.

```python
def handler(event, context):
    sentinel_id = event["sentinel_id"]
    with Logger(
        service="child-service",
        sentinel_id=sentinel_id,
        parent_service=event.get("source_service"),
    ) as logger:
        ...
```

---

## Configuration

The SDK reads its configuration from SSM Parameter Store at init:

| Parameter | Description |
|-----------|-------------|
| `/logsentinel/stream-name` | Kinesis Data Stream name |
| `/logsentinel/dlq-url` | SQS DLQ URL (fallback on persistent Kinesis failure) |

These parameters are provisioned automatically by `logsentinel deploy` (see [logsentinel-cli](https://github.com/logsentinel-io/logsentinel-cli)).

---

## Retry and fallback

On transient Kinesis failures, the SDK retries up to 5 times with exponential backoff (100 ms → 200 ms → 400 ms → 800 ms → 1 600 ms). If all retries are exhausted, failed records are:

1. Printed as JSON to stdout — captured by CloudWatch Logs automatically
2. Sent to the SQS Dead Letter Queue for replay

Override defaults via environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `LOGSENTINEL_RETRY_BASE_MS` | `100` | Base delay in milliseconds |
| `LOGSENTINEL_MAX_RETRIES` | `5` | Maximum number of retry attempts |
