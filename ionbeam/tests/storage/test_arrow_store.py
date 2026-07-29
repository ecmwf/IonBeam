# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

import uuid
from datetime import datetime, timedelta, timezone

import pyarrow as pa
import pytest

from ionbeam.storage.arrow_store import ArrowStore, LocalFileSystemStore, S3ObjectStore


@pytest.fixture(scope="module")
def s3_endpoint():
    from moto.server import ThreadedMotoServer

    server = ThreadedMotoServer(port=0)
    server.start()
    host, port = server.get_host_and_port()
    yield f"http://{host}:{port}"
    server.stop()


@pytest.fixture(params=["local_filesystem", "s3"])
def store(request, tmp_path, monkeypatch) -> ArrowStore:
    if request.param == "local_filesystem":
        return LocalFileSystemStore(tmp_path / "datasets")

    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    return S3ObjectStore(
        bucket=f"test-{uuid.uuid4().hex[:12]}",
        endpoint=request.getfixturevalue("s3_endpoint"),
        region="us-east-1",
    )


def _batch(values) -> pa.RecordBatch:
    return pa.RecordBatch.from_pydict({"value": values})


async def _stream(*batches):
    for batch in batches:
        yield batch


async def _values(store: ArrowStore, key: str) -> list:
    read_back = [b async for b in store.read_record_batches(key)]
    return pa.Table.from_batches(read_back).column("value").to_pylist()


async def test_write_then_read_roundtrips_batches(store):
    rows = await store.write_record_batches(
        "weather/2024", _stream(_batch([1, 2, 3]), _batch([4, 5]))
    )
    assert rows == 5

    assert await _values(store, "weather/2024") == [1, 2, 3, 4, 5]
    assert store.read_schema("weather/2024") == _batch([1]).schema


async def test_read_reslices_to_batch_size(store):
    await store.write_record_batches("weather/2024", _stream(_batch(list(range(10)))))

    read_back = [
        b async for b in store.read_record_batches("weather/2024", batch_size=4)
    ]
    assert [b.num_rows for b in read_back] == [4, 4, 2]


async def test_a_written_key_is_immutable(store):
    await store.write_record_batches("weather/2024", _stream(_batch([1])))
    assert await store.exists("weather/2024")

    with pytest.raises(FileExistsError):
        await store.write_record_batches("weather/2024", _stream(_batch([2])))

    assert await _values(store, "weather/2024") == [1]


async def test_list_keys_scopes_to_prefix(store):
    before = datetime.now(timezone.utc) - timedelta(minutes=1)
    await store.write_record_batches("weather/2024/v1", _stream(_batch([1])))
    await store.write_record_batches("climate/2024/v1", _stream(_batch([2])))

    everything = await store.list_keys("")
    assert [obj.key for obj in everything] == ["climate/2024/v1", "weather/2024/v1"]
    assert all(obj.written_at >= before for obj in everything)

    assert [obj.key for obj in await store.list_keys("weather")] == ["weather/2024/v1"]
    assert await store.list_keys("elsewhere") == []


async def test_delete_removes_key(store):
    await store.write_record_batches("weather/2024", _stream(_batch([1])))
    await store.delete("weather/2024")
    assert not await store.exists("weather/2024")


async def test_empty_stream_writes_nothing(store):
    rows = await store.write_record_batches("weather/2024", _stream())
    assert rows == 0
    assert not await store.exists("weather/2024")


async def test_failed_write_publishes_nothing(store):
    async def dies_mid_stream():
        yield _batch([1])
        raise RuntimeError("build died mid-stream")

    with pytest.raises(RuntimeError):
        await store.write_record_batches("weather/2024", dies_mid_stream())

    assert not await store.exists("weather/2024")
    assert await store.list_keys("") == []
