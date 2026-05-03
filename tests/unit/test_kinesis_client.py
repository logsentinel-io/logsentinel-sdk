from unittest.mock import MagicMock, patch

import boto3

from logsentinel_sdk import KinesisClient, Logger, generate_sentinel_id


def test_success_flush(aws_setup):
    sentinel_id = generate_sentinel_id()
    with Logger(service="test-kinesis", sentinel_id=sentinel_id) as logger:
        logger.info("msg", key='val')
        logger.debug("msg", key='val')
        logger.error("msg", key='val')
        logger.warning("msg", key='val')
        logger.critical("msg", key='val')
    kinesis_client = boto3.client("kinesis")
    shard_iterator = kinesis_client.get_shard_iterator(
        StreamName="logsentinel-stream",
        ShardId="shardId-000000000000",
        ShardIteratorType="TRIM_HORIZON",
    )
    records = kinesis_client.get_records(
        ShardIterator=shard_iterator["ShardIterator"],
    )
    assert len(records["Records"]) == 5

def test_partial_failure(aws_setup, records_test):
    fake_kinesis = MagicMock()
    fake_sqs = MagicMock()
    fake_kinesis.put_records.side_effect = [
      {"FailedRecordCount": 1, "Records": [{"SequenceNumber": "..."},
  {"SequenceNumber": "..."}, {"ErrorCode":
  "ProvisionedThroughputExceededException", "ErrorMessage": "..."}]},
      {"FailedRecordCount": 0, "Records": [{"SequenceNumber": "..."}]},
  ]
    kc = KinesisClient(fake_kinesis, fake_sqs, "logsentinel-stream","https://sqs/dlq" )
    kc.flush(records_test)

    assert fake_kinesis.put_records.call_count == 2
    assert len(fake_kinesis.put_records.call_args_list[1].kwargs["Records"]) == 1

@patch("time.sleep")
@patch("builtins.print")
def test_total_failure(mock_print,mock_sleep, aws_setup, records_test, failed_records):
    fake_kinesis = MagicMock()
    fake_sqs = MagicMock()
    fake_kinesis.put_records.side_effect = failed_records
    kc = KinesisClient(fake_kinesis, fake_sqs, "logsentinel-stream","https://sqs/dlq" )
    kc.flush(records_test)

    assert mock_print.call_count == 3
    assert fake_sqs.send_message.call_count == 3
    assert mock_sleep.call_count == 4
    assert [c.args[0] for c in mock_sleep.call_args_list] == [0.1,0.2,0.4,0.8]


def test_var_env_override(monkeypatch,aws_setup, records_test, failed_records):
    monkeypatch.setenv("LOGSENTINEL_RETRY_BASE_MS", "50")
    monkeypatch.setenv("LOGSENTINEL_MAX_RETRIES", "3")
    fake_kinesis = MagicMock()
    fake_sqs = MagicMock()
    fake_kinesis.put_records.side_effect = failed_records
    kc = KinesisClient(fake_kinesis, fake_sqs, "logsentinel-stream","https://sqs/dlq" )
    kc.flush(records_test)

    assert fake_kinesis.put_records.call_count == 3