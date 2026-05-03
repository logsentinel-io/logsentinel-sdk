import uuid

from logsentinel_sdk import generate_sentinel_id


def test_generate_sentinel_id_name():
    id = generate_sentinel_id()
    assert id.startswith('sent-')

def test_generate_sentinel_id_unite():
    id_without_prefix = generate_sentinel_id().removeprefix('sent-')
    assert uuid.UUID(id_without_prefix, version=4)

def test_generate_sentinel_id_uniqueness():
    test_set = set()
    for i in range(1000):
        test_set.add(generate_sentinel_id())
    assert len(test_set) == 1000