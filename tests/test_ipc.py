from typing import AsyncGenerator
import pyarrow as pa
import pytest
from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from fastapi.testclient import TestClient

from ecoscope_earthranger_io_core.arrow import OBSERVATIONS_SCHEMA_EARTHRANGER
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


def test_streaming_arrow(app: TestClient):
    response = app.get("/stream/arrow")
    assert response.status_code == 200
