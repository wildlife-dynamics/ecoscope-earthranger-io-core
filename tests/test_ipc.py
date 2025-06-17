from typing import AsyncGenerator
import pyarrow as pa
import pytest
from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from fastapi.testclient import TestClient

from ecoscope_earthranger_io_core.arrow import OBSERVATIONS_SCHEMA_EARTHRANGER
from ecoscope_earthranger_io_core.client import get_table
from ecoscope_earthranger_io_core.serve import generate_bytes


@pytest.fixture
def async_batch_generator(
    mock_observations_record_batch: pa.RecordBatch,
) -> AsyncGenerator[pa.RecordBatch, None]:
    async def _async_generator():
        for _ in range(1):  # Simulate a single batch for testing
            yield mock_observations_record_batch

    return _async_generator


@pytest.fixture
def app(async_batch_generator: AsyncGenerator):
    app = FastAPI()

    @app.get("/stream/arrow")
    async def get_observations_streaming_arrow():
        content_stream = generate_bytes(
            earthranger_schema=OBSERVATIONS_SCHEMA_EARTHRANGER,
            async_batch_generator=async_batch_generator(),
            conversion=None,
        )
        try:
            return StreamingResponse(
                content_stream,
                media_type="application/vnd.apache.arrow.stream",
            )
        except Exception as e:
            raise HTTPException(
                status_code=500, detail=f"Failed to read data: {str(e)}"
            )

    with TestClient(app) as client:
        yield client


@pytest.mark.asyncio
async def test_client_get_table(app: TestClient):
    table = await get_table(
        route="/stream/arrow",
        query=None,  # Assuming no query parameters for this test
        base_url=app.base_url,
        headers=None,
    )
    assert isinstance(table, pa.Table)
    assert table.schema.equals(OBSERVATIONS_SCHEMA_EARTHRANGER)
