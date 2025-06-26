from datetime import datetime
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

from conftest import get_async_rb_generator_from_storage_backend

RecordBatchGeneratorGetter = Callable[
    [ObservationsQuery], Callable[[], AsyncIterable[pa.RecordBatch]]
]


@pytest.fixture
def app():
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
            columns=transform.required_columns,
            schema=transform.pre_transform_schema,
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
    query = ObservationsQuery(
        tenant_id="tenant123",
        subject_ids=["subject1", "subject2"],
        range_start=datetime(2023, 1, 1),
        range_end=datetime(2023, 12, 31),
    )
    async with AsyncClient(
        transport=ASGITransport(app),
        base_url="http://test",
    ) as client:
        table = await get_table(
            client=client,
            route="/stream/arrow",
            query=query,
            headers=None,
        )
    # TODO:
    # - [ ] test schema conversion via query parameters
    assert isinstance(table, pa.Table)
    assert table.schema.equals(OBSERVATIONS_SCHEMA__ECOSCOPE_SLIM_V1)
    assert len(table) == nrecords
