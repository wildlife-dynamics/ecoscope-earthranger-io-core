from datetime import datetime
from typing import AsyncIterable, Callable
from unittest.mock import patch

import pyarrow as pa
import pytest
from fastapi import FastAPI
from httpx import AsyncClient, ASGITransport

from ecoscope_earthranger_io_core.arrow import (
    OBSERVATIONS_SCHEMA__ECOSCOPE_SLIM_V1,
)
from ecoscope_earthranger_io_core.client import ERWarehouseClient, _get_table
from ecoscope_earthranger_io_core.query import ObservationsQuery

from _fastapi_example import app as _app

RecordBatchGeneratorGetter = Callable[
    [ObservationsQuery], Callable[[], AsyncIterable[pa.RecordBatch]]
]


@pytest.fixture
def app():
    return _app


@pytest.mark.asyncio
async def test__get_table(app: FastAPI, nrecords: int) -> None:
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
        table = await _get_table(
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


@pytest.mark.asyncio
async def test_client_get_subjectgroup_observations(
    app: FastAPI,
    nrecords: int,
) -> None:
    async with AsyncClient(
        transport=ASGITransport(app),
        base_url="http://test",
    ) as mock_httpx_client:
        with patch(
            "ecoscope_earthranger_io_core.client.ERWarehouseClient._httpx_client",
            return_value=mock_httpx_client,
        ):
            er_client = ERWarehouseClient(
                server="https://some-site.pamdas.org/",
                username="fast-data-enthusiast",
                token="abc",
                _warehouse_base_url="http://test",
                _warehouse_observations_router="/observations",
            )
            table = await er_client.get_subjectgroup_observations(
                subject_group_name="Ecoscope",
                since="2015-01-01T12:00:00+00:00",
                until="2015-03-01T12:00:00+00:00",
            )
            assert isinstance(table, pa.Table)
            assert table.schema.equals(OBSERVATIONS_SCHEMA__ECOSCOPE_SLIM_V1)
            assert len(table) == nrecords
