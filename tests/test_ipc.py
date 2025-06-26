from typing import AsyncIterable, Callable

import pyarrow as pa
import pytest
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import StreamingResponse
from httpx import AsyncClient, ASGITransport

from ecoscope_earthranger_io_core.arrow import (
    OBSERVATIONS_SCHEMA__ECOSCOPE_SLIM_V1,
    TRANSFORMS,
    SchemaChoices,
)
from ecoscope_earthranger_io_core.client import get_table
from ecoscope_earthranger_io_core.query import ObservationsQuery


RecordBatchGeneratorGetter = Callable[
    [ObservationsQuery], Callable[[], AsyncIterable[pa.RecordBatch]]
]


@pytest.fixture
def get_async_rb_generator_from_storage_backend(
    mock_observations_record_batch: pa.RecordBatch,
) -> RecordBatchGeneratorGetter:
    def _get_rb_generator(
        query: ObservationsQuery,
    ) -> AsyncIterable[pa.RecordBatch]:
        async def _async_generator() -> AsyncIterable[pa.RecordBatch]:
            for _ in range(1):  # Simulate a single batch for testing
                yield mock_observations_record_batch

        return _async_generator

    return _get_rb_generator


@pytest.fixture
def app(get_async_rb_generator_from_storage_backend: RecordBatchGeneratorGetter):
    app = FastAPI()

    @app.get("/stream/arrow")
    async def get_observations_streaming_arrow(
        query: ObservationsQuery,
        schema: SchemaChoices = Query(
            "ECOSCOPE_SLIM_V1",
            description="Schema to use for the response",
        ),
    ):
        """Stream observations as an Arrow IPC stream."""

        transform = TRANSFORMS[schema]
        async_batch_generator = get_async_rb_generator_from_storage_backend(
            query,
            columns=transform.required_columns or None,
        )
        content_stream = transform.generate_bytes(
            async_batch_generator=async_batch_generator()
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

    return app


@pytest.mark.asyncio
async def test_client_get_table(app: FastAPI, nrecords: int) -> None:
    async with AsyncClient(
        transport=ASGITransport(app),
        base_url="http://test",
    ) as client:
        table = await get_table(
            client=client,
            route="/stream/arrow",
            query=None,  # Assuming no query parameters for this test
            headers=None,
        )
    # TODO:
    # - [ ] test actual query (and make it required)
    # - [ ] test query w/ column drops
    # - [ ] test schema conversion via query parameters
    assert isinstance(table, pa.Table)
    assert table.schema.equals(OBSERVATIONS_SCHEMA__ECOSCOPE_SLIM_V1)
    assert len(table) == nrecords
