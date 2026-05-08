import datetime
from unittest.mock import patch

from logsentinel_sdk import Logger, generate_sentinel_id


def test_instantiate_mock_logger(aws_setup):
    sentinel_id = generate_sentinel_id()
    with Logger(service="test", sentinel_id=sentinel_id ) as logger:
        assert True
        assert logger._stream_name == "logsentinel-stream"

def test_logger_append_buffer(aws_setup):
    sentinel_id = generate_sentinel_id()
    with Logger(
            service="test",
            sentinel_id=sentinel_id,
            parent_service="game-session"
    ) as logger:
        logger.info("msg", pokemon='pikachu')
        assert logger._buffer[0]["service"] == "test"
        assert logger._buffer[0]["sentinel_id"] == sentinel_id
        assert datetime.datetime.fromisoformat(logger._buffer[0]["timestamp"])
        assert logger._buffer[0]["level"] == "INFO"
        assert logger._buffer[0]["message"] == "msg"
        assert logger._buffer[0]["parent_service"] == "game-session"
        assert logger._buffer[0]["metadata"] == {"pokemon": "pikachu"}
        assert "lambda_request_id" not in logger._buffer[0]

def test_logger_without_parent_service(aws_setup):
    sentinel_id = generate_sentinel_id()
    with Logger(
        service="test",
        sentinel_id=sentinel_id,
    ) as logger:
        logger.info("msg", key='val')
        assert "parent_service" not in logger._buffer[0]

def test_level_string(aws_setup):
    sentinel_id = generate_sentinel_id()
    with Logger(
        service="test",
        sentinel_id=sentinel_id
    ) as logger:
        logger.info("msg", key='val')
        logger.debug("msg", key='val')
        logger.error("msg", key='val')
        logger.warning("msg", key='val')
        logger.critical("msg", key='val')
        assert logger._buffer[0]["level"] == "INFO"
        assert logger._buffer[1]["level"] == "DEBUG"
        assert logger._buffer[2]["level"] == "ERROR"
        assert logger._buffer[3]["level"] == "WARNING"
        assert logger._buffer[4]["level"] == "CRITICAL"

def test_flush(aws_setup):
    sentinel_id = generate_sentinel_id()
    logger = Logger(service="test", sentinel_id=sentinel_id)
    with patch.object(
            logger._kinesis_client.kinesis_client,
            "put_records", wraps= logger._kinesis_client.kinesis_client.put_records) as mock_put_records:
        with logger:
            for _ in range(5):
                logger.info("msg")

    assert mock_put_records.call_count == 1
    assert len(mock_put_records.call_args[1]["Records"]) == 5
    assert len(logger._buffer) == 0

