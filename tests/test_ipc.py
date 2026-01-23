from contextlib import asynccontextmanager
from datetime import datetime
from typing import AsyncIterable, Callable
from unittest.mock import patch

import pyarrow as pa
import pytest
from fastapi import FastAPI
from httpx import AsyncClient, ASGITransport

from ecoscope_earthranger_io_core.arrow import (
    OBSERVATIONS_SCHEMA__ECOSCOPE_SLIM_V1,
    OBSERVATIONS_WITH_PATROL_SCHEMA_SLIM_V1,
    PATROLS_NESTED_SCHEMA_V1,
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
        tenant_domain="some-site.pamdas.org",
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
            route="/observations/stream/arrow",
            query=query,
        )
    # TODO:
    # - [ ] test schema conversion via query parameters
    assert isinstance(table, pa.Table)
    assert table.schema.equals(OBSERVATIONS_SCHEMA__ECOSCOPE_SLIM_V1)
    assert len(table) == nrecords


@pytest.mark.asyncio
async def test__get_table_with_subject_group(app: FastAPI, nrecords: int) -> None:
    """Test _get_table with subject_group_name instead of subject_ids."""
    query = ObservationsQuery(
        tenant_domain="some-site.pamdas.org",
        subject_group_name="elephants",
        range_start=datetime(2023, 1, 1),
        range_end=datetime(2023, 12, 31),
    )
    async with AsyncClient(
        transport=ASGITransport(app),
        base_url="http://test",
    ) as client:
        table = await _get_table(
            client=client,
            route="/observations/stream/arrow",
            query=query,
        )
    assert isinstance(table, pa.Table)
    assert table.schema.equals(OBSERVATIONS_SCHEMA__ECOSCOPE_SLIM_V1)
    assert len(table) == nrecords


def test_client_get_subjectgroup_observations(
    app: FastAPI,
    nrecords: int,
) -> None:
    """Test the sync get_subjectgroup_observations method returns PyArrow Table."""

    @asynccontextmanager
    async def _mock_httpx_client(self):
        async with AsyncClient(
            transport=ASGITransport(app),
            base_url="http://test",
        ) as mock_httpx_client:
            yield mock_httpx_client

    with patch.object(
        ERWarehouseClient,
        "_httpx_client",
        _mock_httpx_client,
    ):
        er_client = ERWarehouseClient(
            server="some-site.pamdas.org",
            username="fast-data-enthusiast",
            token="abc",
            warehouse_base_url="http://test",
        )
        table = er_client.get_subjectgroup_observations(
            subject_group_name="Ecoscope",
            since="2015-01-01T12:00:00",
            until="2015-03-01T12:00:00",
        )
        assert isinstance(table, pa.Table)
        assert len(table) == nrecords
        # Check expected columns from ECOSCOPE_SLIM_V1 schema
        expected_columns = list(OBSERVATIONS_SCHEMA__ECOSCOPE_SLIM_V1.names)
        for col in expected_columns:
            assert col in table.column_names, f"Missing expected column: {col}"


def test_client_get_patrol_observations_with_patrol_filter(
    app: FastAPI,
    nrecords: int,
) -> None:
    """Test the sync get_patrol_observations_with_patrol_filter returns PyArrow Table."""

    @asynccontextmanager
    async def _mock_httpx_client(self):
        async with AsyncClient(
            transport=ASGITransport(app),
            base_url="http://test",
        ) as mock_httpx_client:
            yield mock_httpx_client

    with patch.object(
        ERWarehouseClient,
        "_httpx_client",
        _mock_httpx_client,
    ):
        er_client = ERWarehouseClient(
            server="some-site.pamdas.org",
            username="fast-data-enthusiast",
            token="abc",
            warehouse_base_url="http://test",
        )
        table = er_client.get_patrol_observations_with_patrol_filter(
            since="2015-01-01T12:00:00",
            until="2015-03-01T12:00:00",
            patrol_type_value=["routine_patrol"],
            status=["done"],
            include_patrol_details=True,
        )
        assert isinstance(table, pa.Table)
        assert len(table) == nrecords
        # Check expected columns from ECOSCOPE_SLIM_V1 schema
        expected_columns = list(OBSERVATIONS_WITH_PATROL_SCHEMA_SLIM_V1.names)
        for col in expected_columns:
            assert col in table.column_names, f"Missing expected column: {col}"


def test_client_get_patrols(app: FastAPI) -> None:
    """Test the sync get_patrols method returns PyArrow Table with nested schema."""

    @asynccontextmanager
    async def _mock_httpx_client(self):
        async with AsyncClient(
            transport=ASGITransport(app),
            base_url="http://test",
        ) as mock_httpx_client:
            yield mock_httpx_client

    with patch.object(
        ERWarehouseClient,
        "_httpx_client",
        _mock_httpx_client,
    ):
        er_client = ERWarehouseClient(
            server="some-site.pamdas.org",
            username="fast-data-enthusiast",
            token="abc",
            warehouse_base_url="http://test",
        )
        table = er_client.get_patrols(
            since="2015-01-01T12:00:00",
            until="2015-03-01T12:00:00",
            patrol_type_value=["routine_patrol"],
            status=["done"],
        )
        assert isinstance(table, pa.Table)
        assert len(table) == 3  # Default num_patrols in mock
        # Check expected columns from PATROLS_NESTED_SCHEMA
        expected_columns = list(PATROLS_NESTED_SCHEMA_V1.names)
        for col in expected_columns:
            assert col in table.column_names, f"Missing expected column: {col}"
        # Verify patrol_segments is a list column
        assert "patrol_segments" in table.column_names
        segments = table.column("patrol_segments").to_pylist()
        assert all(isinstance(s, list) for s in segments)


def test_client_get_patrol_observations(app: FastAPI) -> None:
    """Test get_patrol_observations with patrols_df from get_patrols."""

    @asynccontextmanager
    async def _mock_httpx_client(self):
        async with AsyncClient(
            transport=ASGITransport(app),
            base_url="http://test",
        ) as mock_httpx_client:
            yield mock_httpx_client

    with patch.object(
        ERWarehouseClient,
        "_httpx_client",
        _mock_httpx_client,
    ):
        er_client = ERWarehouseClient(
            server="some-site.pamdas.org",
            username="fast-data-enthusiast",
            token="abc",
            warehouse_base_url="http://test",
        )

        # First get patrols
        patrols_table = er_client.get_patrols(
            since="2015-01-01T12:00:00",
            until="2015-03-01T12:00:00",
            patrol_type_value=["routine_patrol"],
            status=["done"],
        )

        # Then get observations for those patrols
        observations_table = er_client.get_patrol_observations(
            patrols_df=patrols_table,
            include_patrol_details=True,
        )

        assert isinstance(observations_table, pa.Table)
        # Time range is derived from patrol segments, so record count varies
        assert len(observations_table) > 0
        # Check expected columns from observations with patrol schema
        expected_columns = list(OBSERVATIONS_WITH_PATROL_SCHEMA_SLIM_V1.names)
        for col in expected_columns:
            assert col in observations_table.column_names, (
                f"Missing expected column: {col}"
            )


def test_client_unsupported_methods_raise_not_implemented() -> None:
    """Test that event-related methods raise NotImplementedError."""
    er_client = ERWarehouseClient(
        server="some-site.pamdas.org",
        token="abc",
        warehouse_base_url="http://test",
    )

    with pytest.raises(NotImplementedError):
        er_client.get_patrol_events()

    with pytest.raises(NotImplementedError):
        er_client.get_events()

    with pytest.raises(NotImplementedError):
        er_client.get_event_types()

    with pytest.raises(NotImplementedError):
        er_client.get_event_type_display_names_from_events(events_gdf=None)
