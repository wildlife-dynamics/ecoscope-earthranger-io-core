from contextlib import asynccontextmanager, contextmanager
from datetime import datetime
from typing import AsyncIterable, Callable
from unittest.mock import AsyncMock, MagicMock, patch

import httpx

from pydantic import SecretStr

import pyarrow as pa
import pytest
from fastapi import FastAPI
from httpx import AsyncClient, ASGITransport

from ecoscope_earthranger_io_core.arrow import (
    EVENT_TYPES_SCHEMA_V1,
    EVENTS_SCHEMA_V1,
    OBSERVATIONS_SCHEMA__ECOSCOPE_SLIM_V1,
    OBSERVATIONS_WITH_PATROL_SCHEMA_SLIM_V1,
    PATROLS_NESTED_SCHEMA_V1,
    PATROLS_WITH_EVENTS_NESTED_SCHEMA_V1,
)
from ecoscope_earthranger_io_core.client import (
    ERWarehouseClient,
    _get_table,
    _search_table,
)
from ecoscope_earthranger_io_core.query import ObservationsQuery

from _fastapi_example import app as _app

RecordBatchGeneratorGetter = Callable[
    [ObservationsQuery], Callable[[], AsyncIterable[pa.RecordBatch]]
]

MOCK_STATUS_RESPONSE = {
    "data": {
        "dwh_settings": {
            "api_url": "https://warehouse-api-dev-123.europe-west3.run.app"
        },
    },
    "status": {"code": 200, "message": "OK"},
}


@pytest.fixture
def app():
    return _app


@pytest.fixture(autouse=True)
def _mock_id_token():
    """Patch _get_id_token for all tests so Google credentials are not required."""
    with patch.object(
        ERWarehouseClient,
        "_get_id_token",
        return_value=SecretStr("mock-id-token"),
    ):
        yield


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


@pytest.mark.asyncio
async def test__get_table_raises_on_empty_stream() -> None:
    """Test that _get_table raises ConnectionError when the stream is empty."""
    query = ObservationsQuery(
        tenant_domain="some-site.pamdas.org",
        subject_ids=["subject1"],
        range_start=datetime(2023, 1, 1),
        range_end=datetime(2023, 12, 31),
    )

    async def empty_response(scope, receive, send):
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b""})

    async with AsyncClient(
        transport=ASGITransport(empty_response),
        base_url="http://test",
    ) as client:
        with pytest.raises(ConnectionError, match="stream broke"):
            await _get_table(
                client=client,
                route="/observations/stream/arrow",
                query=query,
            )


def _sent(captured: dict) -> dict:
    """Everything the client put on the wire, body and query string merged.

    Filters now travel in the JSON body of the POST /search routes while
    response-shaping options (store_type) stay in the query string. These
    tests assert *that* a flag was forwarded; the dedicated transport tests
    below assert *where* it was forwarded.
    """
    return {**(captured.get("query_params") or {}), **(captured.get("body") or {})}


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

        # ERDW-264: groupby_col must carry the patrol id (one trajectory per
        # patrol), not the leader subject id, so trajectories aren't collapsed
        # by leader.
        assert (
            table.column("groupby_col").to_pylist()
            == table.column("patrol_id").to_pylist()
        )
        # The fixture maps each leader subject to a distinct patrol, so the
        # distinct-group count must equal the distinct-patrol count.
        assert len(set(table.column("groupby_col").to_pylist())) == len(
            set(table.column("patrol_id").to_pylist())
        )
        # ERDW-264: patrol_subject (leader name) must be populated, not null —
        # this is the trajectory-legend color column.
        patrol_subject = table.column("patrol_subject").to_pylist()
        assert all(v == "mock-subject-name" for v in patrol_subject)
        # The leader-subject columns from the subject-group schema must not
        # leak into the patrol schema.
        assert "extra__subject__name" not in table.column_names
        assert "extra__subject__subject_subtype" not in table.column_names


def test_client_get_patrols_minimal(app: FastAPI) -> None:
    """Test the sync get_patrols_minimal method returns PyArrow Table with nested schema."""

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
        table = er_client.get_patrols_minimal(
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


def test_client_get_patrols_with_events(app: FastAPI) -> None:
    """get_patrols returns a pa.Table of patrols with events nested under each
    patrol segment (PATROLS_WITH_EVENTS_NESTED_SCHEMA_V1)."""

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
    assert table.schema == PATROLS_WITH_EVENTS_NESTED_SCHEMA_V1
    assert table.num_rows > 0
    assert "patrol_segments" in table.column_names

    segments = table.column("patrol_segments").to_pylist()[0]
    assert isinstance(segments, list)
    # events are nested under each segment.
    assert "events" in segments[0]
    events = segments[0]["events"]
    assert isinstance(events, list)
    assert "geometry" in events[0]


def test_client_get_patrol_observations(app: FastAPI) -> None:
    """Test get_patrol_observations with patrols_df from get_patrols_minimal."""

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

        # First get patrols (minimal data without events)
        patrols_table = er_client.get_patrols_minimal(
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

        # ERDW-264: groupby_col carries patrol id, patrol_subject is populated,
        # and the subject-group leader columns are absent.
        assert (
            observations_table.column("groupby_col").to_pylist()
            == observations_table.column("patrol_id").to_pylist()
        )
        assert all(
            v == "mock-subject-name"
            for v in observations_table.column("patrol_subject").to_pylist()
        )
        assert "extra__subject__name" not in observations_table.column_names
        assert "extra__subject__subject_subtype" not in observations_table.column_names


def test_client_query_engine_default_auto(
    app: FastAPI,
    nrecords: int,
) -> None:
    """Test that the default query_engine='auto' passes store_type=auto."""
    captured_params: dict = {}

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
            token="abc",
            warehouse_base_url="http://test",
        )
        assert er_client.query_engine == "auto"

        original_search_table = _search_table

        async def _capturing_search_table(*args, **kwargs):
            captured_params["store_type"] = kwargs.get("store_type")
            return await original_search_table(*args, **kwargs)

        with patch(
            "ecoscope_earthranger_io_core.client._search_table",
            side_effect=_capturing_search_table,
        ):
            table = er_client.get_subjectgroup_observations(
                subject_group_name="Ecoscope",
                since="2015-01-01T12:00:00",
                until="2015-03-01T12:00:00",
            )
        assert isinstance(table, pa.Table)
        assert captured_params["store_type"] == "auto"


def test_client_query_engine_explicit_per_request(
    app: FastAPI,
    nrecords: int,
) -> None:
    """Test that per-request query_engine overrides the client default."""
    captured_params: dict = {}

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
            token="abc",
            warehouse_base_url="http://test",
        )

        original_search_table = _search_table

        async def _capturing_search_table(*args, **kwargs):
            captured_params["store_type"] = kwargs.get("store_type")
            return await original_search_table(*args, **kwargs)

        with patch(
            "ecoscope_earthranger_io_core.client._search_table",
            side_effect=_capturing_search_table,
        ):
            table = er_client.get_subjectgroup_observations(
                subject_group_name="Ecoscope",
                since="2015-01-01T12:00:00",
                until="2015-03-01T12:00:00",
                query_engine="iceberg-bq",
            )
        assert isinstance(table, pa.Table)
        assert captured_params["store_type"] == "iceberg-bq"


def test_client_query_engine_client_level_default(
    app: FastAPI,
    nrecords: int,
) -> None:
    """Test that the client-level query_engine is used when no per-request override."""
    captured_params: dict = {}

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
            token="abc",
            warehouse_base_url="http://test",
            query_engine="iceberg-dd",
        )

        original_search_table = _search_table

        async def _capturing_search_table(*args, **kwargs):
            captured_params["store_type"] = kwargs.get("store_type")
            return await original_search_table(*args, **kwargs)

        with patch(
            "ecoscope_earthranger_io_core.client._search_table",
            side_effect=_capturing_search_table,
        ):
            table = er_client.get_subjectgroup_observations(
                subject_group_name="Ecoscope",
                since="2015-01-01T12:00:00",
                until="2015-03-01T12:00:00",
            )
        assert isinstance(table, pa.Table)
        assert captured_params["store_type"] == "iceberg-dd"


def test_client_query_engine_per_request_overrides_client_default(
    app: FastAPI,
    nrecords: int,
) -> None:
    """Test that per-request query_engine overrides the client-level default."""
    captured_params: dict = {}

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
            token="abc",
            warehouse_base_url="http://test",
            query_engine="iceberg-dd",
        )

        original_search_table = _search_table

        async def _capturing_search_table(*args, **kwargs):
            captured_params["store_type"] = kwargs.get("store_type")
            return await original_search_table(*args, **kwargs)

        with patch(
            "ecoscope_earthranger_io_core.client._search_table",
            side_effect=_capturing_search_table,
        ):
            table = er_client.get_subjectgroup_observations(
                subject_group_name="Ecoscope",
                since="2015-01-01T12:00:00",
                until="2015-03-01T12:00:00",
                query_engine="iceberg-bq",
            )
        assert isinstance(table, pa.Table)
        assert captured_params["store_type"] == "iceberg-bq"


def test_client_get_patrol_events(app: FastAPI) -> None:
    """get_patrol_events flattens get_patrols' nested events to a flat pa.Table,
    one row per event, with patrol/segment context attached."""
    from ecoscope_earthranger_io_core.arrow import PATROL_EVENTS_FLAT_SCHEMA_V1

    @asynccontextmanager
    async def _mock_httpx_client(self):
        async with AsyncClient(
            transport=ASGITransport(app),
            base_url="http://test",
        ) as mock_httpx_client:
            yield mock_httpx_client

    with patch.object(ERWarehouseClient, "_httpx_client", _mock_httpx_client):
        er_client = ERWarehouseClient(
            server="some-site.pamdas.org",
            token="abc",
            warehouse_base_url="http://test",
        )
        table = er_client.get_patrol_events(
            since="2015-01-01T12:00:00",
            until="2015-03-01T12:00:00",
        )

    assert isinstance(table, pa.Table)
    assert table.schema.equals(PATROL_EVENTS_FLAT_SCHEMA_V1)
    # the canned example app has one patrol -> one segment -> one event
    assert table.num_rows == 1
    row = table.to_pylist()[0]
    assert row["event_type"] == "wildlife_sighting"
    assert row["patrol_id"] == "patrol1"
    assert row["patrol_segment_id"] == "segment1"
    assert row["geometry"] is not None


def test_client_get_patrol_events_event_type_filter(app: FastAPI) -> None:
    """A non-matching event_type filter yields an empty (schema-typed) table."""

    @asynccontextmanager
    async def _mock_httpx_client(self):
        async with AsyncClient(
            transport=ASGITransport(app),
            base_url="http://test",
        ) as mock_httpx_client:
            yield mock_httpx_client

    with patch.object(ERWarehouseClient, "_httpx_client", _mock_httpx_client):
        er_client = ERWarehouseClient(
            server="some-site.pamdas.org",
            token="abc",
            warehouse_base_url="http://test",
        )
        table = er_client.get_patrol_events(event_type=["nonexistent_type"])

    assert isinstance(table, pa.Table)
    assert table.num_rows == 0


# -------------------------------------------------------------------------
# Events tests
# -------------------------------------------------------------------------


def _events_mock_httpx_client(app: FastAPI):
    @asynccontextmanager
    async def _mock_httpx_client(self):
        async with AsyncClient(
            transport=ASGITransport(app),
            base_url="http://test",
        ) as mock_httpx_client:
            yield mock_httpx_client

    return _mock_httpx_client


def _capturing_mock_httpx_client(app: FastAPI, captured: dict):
    @asynccontextmanager
    async def _mock_httpx_client(self):
        async with AsyncClient(
            transport=ASGITransport(app),
            base_url="http://test",
        ) as mock_httpx_client:
            original_stream = mock_httpx_client.stream

            def _capturing_stream(method, url, **kwargs):
                captured["method"] = method
                captured["url"] = url
                captured["query_params"] = kwargs.get("params")
                captured["body"] = kwargs.get("json")
                return original_stream(method, url, **kwargs)

            mock_httpx_client.stream = _capturing_stream  # type: ignore[assignment]
            yield mock_httpx_client

    return _mock_httpx_client


def test_client_get_events(app: FastAPI) -> None:
    """Typed events fetch returns a pa.Table with EVENTS_SCHEMA_V1 columns."""
    with patch.object(
        ERWarehouseClient, "_httpx_client", _events_mock_httpx_client(app)
    ):
        er_client = ERWarehouseClient(
            server="some-site.pamdas.org",
            token="abc",
            warehouse_base_url="http://test",
        )
        table = er_client.get_events(
            since="2015-01-01T00:00:00",
            until="2015-03-01T00:00:00",
            event_type=["wildlife_sighting"],
            include_details=True,
        )
    assert isinstance(table, pa.Table)
    for col in EVENTS_SCHEMA_V1.names:
        assert col in table.column_names, f"Missing expected column: {col}"
    assert pa.types.is_struct(table.schema.field("reported_by").type)
    assert pa.types.is_timestamp(table.schema.field("event_time").type)
    reported_by = table.column("reported_by").to_pylist()
    first = reported_by[0]
    assert first is not None
    assert first["name"] == "Ranger A"


def test_client_get_events_raw_multi_type(app: FastAPI) -> None:
    """raw_details=True streams the flat JSON form across multiple event types."""
    captured: dict = {}

    @asynccontextmanager
    async def _mock_httpx_client(self):
        async with AsyncClient(
            transport=ASGITransport(app),
            base_url="http://test",
        ) as mock_httpx_client:
            original_stream = mock_httpx_client.stream

            def _capturing_stream(method, url, **kwargs):
                captured["method"] = method
                captured["url"] = url
                captured["query_params"] = kwargs.get("params")
                captured["body"] = kwargs.get("json")
                return original_stream(method, url, **kwargs)

            mock_httpx_client.stream = _capturing_stream  # type: ignore[assignment]
            yield mock_httpx_client

    with patch.object(ERWarehouseClient, "_httpx_client", _mock_httpx_client):
        er_client = ERWarehouseClient(
            server="some-site.pamdas.org",
            token="abc",
            warehouse_base_url="http://test",
        )
        table = er_client.get_events(
            since="2015-01-01T00:00:00",
            until="2015-03-01T00:00:00",
            event_type=["a", "b"],
            raw_details=True,
        )
    assert isinstance(table, pa.Table)
    assert len(table) > 0
    assert _sent(captured)["raw_details"] is True
    # raw details are still details, so the payload is included
    assert _sent(captured)["include_details"] is True


def test_client_get_events_no_details_default(app: FastAPI) -> None:
    """Default (no include_details / raw_details / typed flags) omits the
    event_details payload and works across multiple event types without error."""
    captured: dict = {}
    with patch.object(
        ERWarehouseClient, "_httpx_client", _capturing_mock_httpx_client(app, captured)
    ):
        er_client = ERWarehouseClient(
            server="some-site.pamdas.org",
            token="abc",
            warehouse_base_url="http://test",
        )
        er_client.get_events(
            since="2015-01-01T00:00:00",
            until="2015-03-01T00:00:00",
            event_type=["a", "b"],
        )
    assert _sent(captured)["include_details"] is False
    assert _sent(captured)["raw_details"] is False


def test_client_get_events_typed_path_params(app: FastAPI) -> None:
    """Typed mode (one event_type) must NOT send raw_details; typed flags pass through."""
    captured: dict = {}
    with patch.object(
        ERWarehouseClient, "_httpx_client", _capturing_mock_httpx_client(app, captured)
    ):
        er_client = ERWarehouseClient(
            server="some-site.pamdas.org",
            token="abc",
            warehouse_base_url="http://test",
        )
        er_client.get_events(
            since="2015-01-01T00:00:00",
            until="2015-03-01T00:00:00",
            event_type=["wildlife_sighting"],
            include_details=True,
        )
    assert _sent(captured)["include_details"] is True
    assert _sent(captured)["raw_details"] is False

    captured.clear()
    with patch.object(
        ERWarehouseClient, "_httpx_client", _capturing_mock_httpx_client(app, captured)
    ):
        er_client = ERWarehouseClient(
            server="some-site.pamdas.org",
            token="abc",
            warehouse_base_url="http://test",
        )
        er_client.get_events(
            since="2015-01-01T00:00:00",
            until="2015-03-01T00:00:00",
            event_type=["wildlife_sighting"],
            parse_detail_datetimes=True,
            invalid_only=True,
        )
    assert _sent(captured)["raw_details"] is False
    assert _sent(captured)["parse_detail_datetimes"] is True
    assert _sent(captured)["invalid_only"] is True


def test_client_get_events_optional_time_range(app: FastAPI) -> None:
    """since/until are optional (parity with the API and EarthRangerIO): an
    omitted bound is dropped from the query params, and a fully-unbounded call
    still works."""
    captured: dict = {}
    with patch.object(
        ERWarehouseClient, "_httpx_client", _capturing_mock_httpx_client(app, captured)
    ):
        er_client = ERWarehouseClient(
            server="some-site.pamdas.org",
            token="abc",
            warehouse_base_url="http://test",
        )
        # no since/until at all
        table = er_client.get_events(event_type=["wildlife_sighting"])
    assert isinstance(table, pa.Table)
    assert "range_start" not in _sent(captured)
    assert "range_end" not in _sent(captured)

    # half-bounded (only since) is allowed too
    captured.clear()
    with patch.object(
        ERWarehouseClient, "_httpx_client", _capturing_mock_httpx_client(app, captured)
    ):
        er_client = ERWarehouseClient(
            server="some-site.pamdas.org",
            token="abc",
            warehouse_base_url="http://test",
        )
        er_client.get_events(since="2015-01-01T00:00:00", event_type=["a", "b"])
    assert "range_start" in _sent(captured)
    assert "range_end" not in _sent(captured)


def test_client_get_events_forwards_invalid_details(app: FastAPI) -> None:
    """invalid_details='coerce' (typed mode) must reach the query params."""
    captured: dict = {}
    with patch.object(
        ERWarehouseClient, "_httpx_client", _capturing_mock_httpx_client(app, captured)
    ):
        er_client = ERWarehouseClient(
            server="some-site.pamdas.org",
            token="abc",
            warehouse_base_url="http://test",
        )
        er_client.get_events(
            since="2015-01-01T00:00:00",
            until="2015-03-01T00:00:00",
            event_type=["wildlife_sighting"],
            include_details=True,
            invalid_details="coerce",
        )
    assert _sent(captured)["invalid_details"] == "coerce"


def test_client_get_events_rejects_unsupported() -> None:
    """include_updates raises NotImplementedError; typed flags require one type."""
    er_client = ERWarehouseClient(
        server="some-site.pamdas.org",
        token="abc",
        warehouse_base_url="http://test",
    )

    with pytest.raises(NotImplementedError):
        er_client.get_events(
            since="2015-01-01T00:00:00",
            until="2015-03-01T00:00:00",
            include_updates=True,
        )

    with pytest.raises(NotImplementedError):
        er_client.get_events(
            since="2015-01-01T00:00:00",
            until="2015-03-01T00:00:00",
            include_related_events=True,
        )

    with pytest.raises(ValueError):
        er_client.get_events(
            since="2015-01-01T00:00:00",
            until="2015-03-01T00:00:00",
            event_type=["a", "b"],
            parse_detail_datetimes=True,
        )

    # invalid_details is typed-only too: it must reject multiple event types
    # rather than being silently dropped.
    with pytest.raises(ValueError):
        er_client.get_events(
            since="2015-01-01T00:00:00",
            until="2015-03-01T00:00:00",
            event_type=["a", "b"],
            invalid_details="drop",
        )

    # include_details (typed) must error on multiple event types, NOT silently
    # degrade to raw.
    with pytest.raises(ValueError):
        er_client.get_events(
            since="2015-01-01T00:00:00",
            until="2015-03-01T00:00:00",
            event_type=["a", "b"],
            include_details=True,
        )

    # raw_details is mutually exclusive with the typed-detail options.
    with pytest.raises(ValueError):
        er_client.get_events(
            since="2015-01-01T00:00:00",
            until="2015-03-01T00:00:00",
            event_type=["wildlife_sighting"],
            raw_details=True,
            parse_detail_datetimes=True,
        )
    with pytest.raises(ValueError):
        er_client.get_events(
            since="2015-01-01T00:00:00",
            until="2015-03-01T00:00:00",
            event_type=["wildlife_sighting"],
            raw_details=True,
            invalid_details="drop",
        )


def test_client_get_events_include_and_raw_details_compose(app: FastAPI) -> None:
    """include_details=True + raw_details=True is valid: details included but raw
    JSON, so it works across multiple event types and sends raw_details."""
    captured: dict = {}
    with patch.object(
        ERWarehouseClient, "_httpx_client", _capturing_mock_httpx_client(app, captured)
    ):
        er_client = ERWarehouseClient(
            server="some-site.pamdas.org",
            token="abc",
            warehouse_base_url="http://test",
        )
        er_client.get_events(
            since="2015-01-01T00:00:00",
            until="2015-03-01T00:00:00",
            event_type=["a", "b"],
            include_details=True,
            raw_details=True,
        )
    assert _sent(captured)["raw_details"] is True
    assert _sent(captured)["include_details"] is True


def test_client_get_events_invalid_details_triggers_typed_mode(app: FastAPI) -> None:
    """invalid_details alone (no include_details) must enable typed mode and
    reach the query params, not be silently dropped onto the raw path."""
    captured: dict = {}
    with patch.object(
        ERWarehouseClient, "_httpx_client", _capturing_mock_httpx_client(app, captured)
    ):
        er_client = ERWarehouseClient(
            server="some-site.pamdas.org",
            token="abc",
            warehouse_base_url="http://test",
        )
        er_client.get_events(
            since="2015-01-01T00:00:00",
            until="2015-03-01T00:00:00",
            event_type=["wildlife_sighting"],
            invalid_details="drop",
        )
    assert _sent(captured)["invalid_details"] == "drop"
    assert _sent(captured)["raw_details"] is False
    assert _sent(captured)["include_details"] is True


def test_client_get_event_types(app: FastAPI) -> None:
    """get_event_types returns a pa.Table with the 6 columns and 2 canned rows."""
    with patch.object(
        ERWarehouseClient, "_httpx_client", _events_mock_httpx_client(app)
    ):
        er_client = ERWarehouseClient(
            server="some-site.pamdas.org",
            token="abc",
            warehouse_base_url="http://test",
        )
        table = er_client.get_event_types()
    assert isinstance(table, pa.Table)
    assert table.column_names == list(EVENT_TYPES_SCHEMA_V1.names)
    assert len(table) == 2


def test_client_get_event_schema_arrow(app: FastAPI) -> None:
    """get_event_schema (default arrow) returns a pa.Schema whose event_details
    is the derived struct."""
    with patch.object(
        ERWarehouseClient, "_httpx_client", _events_mock_httpx_client(app)
    ):
        er_client = ERWarehouseClient(
            server="some-site.pamdas.org",
            token="abc",
            warehouse_base_url="http://test",
        )
        schema = er_client.get_event_schema("wildlife_sighting")
    assert isinstance(schema, pa.Schema)
    details = schema.field("event_details").type
    assert pa.types.is_struct(details)
    names = {details.field(i).name for i in range(details.num_fields)}
    assert names == {"species", "count", "seen_at"}
    # default (no parse_detail_datetimes) -> the date-time leaf stays a string
    assert details.field("seen_at").type == pa.string()


def test_client_get_event_schema_parse_datetimes(app: FastAPI) -> None:
    """parse_detail_datetimes types the date-time leaf as timestamp[ns, UTC]."""
    with patch.object(
        ERWarehouseClient, "_httpx_client", _events_mock_httpx_client(app)
    ):
        er_client = ERWarehouseClient(
            server="some-site.pamdas.org",
            token="abc",
            warehouse_base_url="http://test",
        )
        schema = er_client.get_event_schema(
            "wildlife_sighting", parse_detail_datetimes=True
        )
    assert isinstance(schema, pa.Schema)
    details = schema.field("event_details").type
    assert details.field("seen_at").type == pa.timestamp("ns", tz="UTC")


def test_client_get_event_schema_json(app: FastAPI) -> None:
    """format="json" returns an informational {field: type_str} mapping."""
    with patch.object(
        ERWarehouseClient, "_httpx_client", _events_mock_httpx_client(app)
    ):
        er_client = ERWarehouseClient(
            server="some-site.pamdas.org",
            token="abc",
            warehouse_base_url="http://test",
        )
        result = er_client.get_event_schema("wildlife_sighting", format="json")
    assert isinstance(result, dict)
    assert set(result) == {"species", "count", "seen_at"}


def test_client_get_event_type_display_names_not_implemented() -> None:
    """Display-name enrichment is a DataFrame op; the client returns only
    pyarrow types, so this raises (resolve via get_event_types() in a task)."""
    er_client = ERWarehouseClient(
        server="some-site.pamdas.org",
        token="abc",
        warehouse_base_url="http://test",
    )
    with pytest.raises(NotImplementedError):
        er_client.get_event_type_display_names_from_events(object())


# -------------------------------------------------------------------------
# Multi-region / URL resolution tests
# -------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_resolve_warehouse_url_from_status() -> None:
    """Test that the warehouse URL is resolved from the status endpoint."""
    mock_response = MagicMock()
    mock_response.json.return_value = MOCK_STATUS_RESPONSE
    mock_response.raise_for_status.return_value = None

    er_client = ERWarehouseClient(
        server="some-site.pamdas.org",
        token="abc",
    )

    mock_ctx = AsyncMock()
    mock_ctx.get.return_value = mock_response

    with patch("httpx.AsyncClient") as mock_client_cls:
        mock_instance = AsyncMock()
        mock_instance.__aenter__.return_value = mock_ctx
        mock_instance.__aexit__.return_value = False
        mock_client_cls.return_value = mock_instance

        url = await er_client._resolve_warehouse_url()

    assert url == "https://warehouse-api-dev-123.europe-west3.run.app"
    assert er_client._resolved_base_url == url


@pytest.mark.asyncio
async def test_resolve_warehouse_url_caches_result() -> None:
    """Test that repeated calls return the cached URL without extra HTTP calls."""
    mock_response = MagicMock()
    mock_response.json.return_value = MOCK_STATUS_RESPONSE
    mock_response.raise_for_status.return_value = None

    er_client = ERWarehouseClient(
        server="some-site.pamdas.org",
        token="abc",
    )

    mock_ctx = AsyncMock()
    mock_ctx.get.return_value = mock_response

    with patch("httpx.AsyncClient") as mock_client_cls:
        mock_instance = AsyncMock()
        mock_instance.__aenter__.return_value = mock_ctx
        mock_instance.__aexit__.return_value = False
        mock_client_cls.return_value = mock_instance

        url1 = await er_client._resolve_warehouse_url()
        url2 = await er_client._resolve_warehouse_url()

    assert url1 == url2
    mock_ctx.get.assert_called_once()


@pytest.mark.asyncio
async def test_resolve_warehouse_url_override_skips_status() -> None:
    """Test that an explicit warehouse_base_url skips the status endpoint."""
    er_client = ERWarehouseClient(
        server="some-site.pamdas.org",
        token="abc",
        warehouse_base_url="https://my-override.example.com",
    )

    with patch("httpx.AsyncClient") as mock_client_cls:
        url = await er_client._resolve_warehouse_url()

    assert url == "https://my-override.example.com"
    mock_client_cls.assert_not_called()


def test_get_auth_headers_includes_both_tokens() -> None:
    """Test that auth headers include both ER token and Google ID token."""
    er_client = ERWarehouseClient(
        server="some-site.pamdas.org",
        token="abc",
        warehouse_base_url="https://warehouse.example.com",
    )
    headers = er_client._get_auth_headers()

    assert headers["X-EarthRanger-API-Token"] == "abc"
    assert headers["Authorization"] == "Bearer mock-id-token"


def test_get_auth_headers_raises_on_invalid_warehouse_url() -> None:
    """Test that _get_auth_headers raises ValueError when hostname cannot be extracted."""
    er_client = ERWarehouseClient(
        server="some-site.pamdas.org",
        token="abc",
        warehouse_base_url="not-a-url",
    )
    with pytest.raises(ValueError, match="Could not extract a valid hostname"):
        er_client._get_auth_headers()


def test_warehouse_base_url_is_optional() -> None:
    """Test that ERWarehouseClient can be constructed without warehouse_base_url."""
    er_client = ERWarehouseClient(
        server="some-site.pamdas.org",
        token="abc",
    )
    assert er_client.warehouse_base_url is None


@pytest.mark.parametrize(
    "raw_server, expected",
    [
        ("site.pamdas.org", "site.pamdas.org"),
        ("site.pamdas.org/", "site.pamdas.org"),
        ("https://site.pamdas.org", "site.pamdas.org"),
        ("http://site.pamdas.org", "site.pamdas.org"),
        ("https://site.pamdas.org/", "site.pamdas.org"),
        ("https://site.pamdas.org/api/v1.0", "site.pamdas.org"),
        ("https://site.pamdas.org/api/v1.0/", "site.pamdas.org"),
        # Schemeless inputs with a path must also be sanitized.
        ("site.pamdas.org/api/v1.0", "site.pamdas.org"),
        ("site.pamdas.org/api/v1.0/", "site.pamdas.org"),
        # Surrounding whitespace is tolerated.
        ("  https://site.pamdas.org/  ", "site.pamdas.org"),
        # urlparse lowercases the hostname component.
        ("HTTPS://Site.Pamdas.Org/", "site.pamdas.org"),
        # Non-default ports are preserved, with or without scheme/path.
        ("site.pamdas.org:8443/api/v1.0/", "site.pamdas.org:8443"),
        ("https://site.pamdas.org:8443", "site.pamdas.org:8443"),
    ],
)
def test_server_field_is_normalized(raw_server: str, expected: str) -> None:
    """Test that the server field strips scheme, path, and trailing slashes."""
    er_client = ERWarehouseClient(
        server=raw_server,
        token="abc",
        warehouse_base_url="http://test",
    )
    assert er_client.server == expected


@pytest.mark.parametrize(
    "raw_server",
    [
        "",
        "   ",
        "://nohost",
    ],
)
def test_server_field_rejects_invalid_input(raw_server: str) -> None:
    """Invalid server inputs should raise a pydantic ValidationError."""
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        ERWarehouseClient(
            server=raw_server,
            token="abc",
            warehouse_base_url="http://test",
        )


# -------------------------------------------------------------------------
# exclusion_flags forwarding tests
# -------------------------------------------------------------------------


@pytest.mark.parametrize("value", [None, 0, 1, 2, 3])
def test_client_get_subjectgroup_observations_forwards_exclusion_flags(
    app: FastAPI,
    value,
) -> None:
    """``exclusion_flags`` should reach the query string of the mocked backend."""
    captured: dict = {}

    @asynccontextmanager
    async def _mock_httpx_client(self):
        async with AsyncClient(
            transport=ASGITransport(app),
            base_url="http://test",
        ) as mock_httpx_client:
            original_stream = mock_httpx_client.stream

            def _capturing_stream(method, url, **kwargs):
                captured["method"] = method
                captured["url"] = url
                captured["query_params"] = kwargs.get("params")
                captured["body"] = kwargs.get("json")
                return original_stream(method, url, **kwargs)

            mock_httpx_client.stream = _capturing_stream  # type: ignore[assignment]
            yield mock_httpx_client

    with patch.object(
        ERWarehouseClient,
        "_httpx_client",
        _mock_httpx_client,
    ):
        er_client = ERWarehouseClient(
            server="some-site.pamdas.org",
            token="abc",
            warehouse_base_url="http://test",
        )
        er_client.get_subjectgroup_observations(
            subject_group_name="Ecoscope",
            since="2015-01-01T12:00:00",
            until="2015-03-01T12:00:00",
            filter=value,
        )

    params = _sent(captured)
    if value is None:
        assert "exclusion_flags" not in params
    else:
        assert params["exclusion_flags"] == value


@pytest.mark.parametrize("value", [None, 0, 1, 2, 3])
def test_client_get_patrol_observations_with_patrol_filter_forwards_exclusion_flags(
    app: FastAPI,
    value,
) -> None:
    """patrol-filtered observations must also forward ``exclusion_flags``."""
    captured: dict = {}

    @asynccontextmanager
    async def _mock_httpx_client(self):
        async with AsyncClient(
            transport=ASGITransport(app),
            base_url="http://test",
        ) as mock_httpx_client:
            original_stream = mock_httpx_client.stream

            def _capturing_stream(method, url, **kwargs):
                captured["method"] = method
                captured["url"] = url
                captured["query_params"] = kwargs.get("params")
                captured["body"] = kwargs.get("json")
                return original_stream(method, url, **kwargs)

            mock_httpx_client.stream = _capturing_stream  # type: ignore[assignment]
            yield mock_httpx_client

    with patch.object(
        ERWarehouseClient,
        "_httpx_client",
        _mock_httpx_client,
    ):
        er_client = ERWarehouseClient(
            server="some-site.pamdas.org",
            token="abc",
            warehouse_base_url="http://test",
        )
        er_client.get_patrol_observations_with_patrol_filter(
            since="2015-01-01T12:00:00",
            until="2015-03-01T12:00:00",
            patrol_type_value=["routine_patrol"],
            status=["done"],
            include_patrol_details=True,
            filter=value,
        )

    params = _sent(captured)
    if value is None:
        assert "exclusion_flags" not in params
    else:
        assert params["exclusion_flags"] == value


# -------------------------------------------------------------------------
# patrols_overlap_daterange forwarding tests
# -------------------------------------------------------------------------


@pytest.mark.parametrize("value", [True, False])
def test_client_get_patrol_observations_with_patrol_filter_forwards_patrols_overlap_daterange(
    app: FastAPI,
    value,
) -> None:
    """patrol-filtered observations must forward ``patrols_overlap_daterange``."""
    captured: dict = {}

    @asynccontextmanager
    async def _mock_httpx_client(self):
        async with AsyncClient(
            transport=ASGITransport(app),
            base_url="http://test",
        ) as mock_httpx_client:
            original_stream = mock_httpx_client.stream

            def _capturing_stream(method, url, **kwargs):
                captured["method"] = method
                captured["url"] = url
                captured["query_params"] = kwargs.get("params")
                captured["body"] = kwargs.get("json")
                return original_stream(method, url, **kwargs)

            mock_httpx_client.stream = _capturing_stream  # type: ignore[assignment]
            yield mock_httpx_client

    with patch.object(
        ERWarehouseClient,
        "_httpx_client",
        _mock_httpx_client,
    ):
        er_client = ERWarehouseClient(
            server="some-site.pamdas.org",
            token="abc",
            warehouse_base_url="http://test",
        )
        er_client.get_patrol_observations_with_patrol_filter(
            since="2015-01-01T12:00:00",
            until="2015-03-01T12:00:00",
            patrol_type_value=["routine_patrol"],
            status=["done"],
            patrols_overlap_daterange=value,
            include_patrol_details=True,
        )

    params = _sent(captured)
    assert params["patrols_overlap_daterange"] == value


def test_client_get_patrol_observations_with_patrol_filter_default_patrols_overlap_daterange_is_true(
    app: FastAPI,
) -> None:
    """When the caller omits ``patrols_overlap_daterange`` it should default to True."""
    captured: dict = {}

    @asynccontextmanager
    async def _mock_httpx_client(self):
        async with AsyncClient(
            transport=ASGITransport(app),
            base_url="http://test",
        ) as mock_httpx_client:
            original_stream = mock_httpx_client.stream

            def _capturing_stream(method, url, **kwargs):
                captured["method"] = method
                captured["url"] = url
                captured["query_params"] = kwargs.get("params")
                captured["body"] = kwargs.get("json")
                return original_stream(method, url, **kwargs)

            mock_httpx_client.stream = _capturing_stream  # type: ignore[assignment]
            yield mock_httpx_client

    with patch.object(
        ERWarehouseClient,
        "_httpx_client",
        _mock_httpx_client,
    ):
        er_client = ERWarehouseClient(
            server="some-site.pamdas.org",
            token="abc",
            warehouse_base_url="http://test",
        )
        er_client.get_patrol_observations_with_patrol_filter(
            since="2015-01-01T12:00:00",
            until="2015-03-01T12:00:00",
            patrol_type_value=["routine_patrol"],
            status=["done"],
            include_patrol_details=True,
        )

    params = _sent(captured)
    assert params["patrols_overlap_daterange"] is True


@pytest.mark.parametrize("value", [True, False])
def test_client_get_patrols_minimal_forwards_patrols_overlap_daterange(
    app: FastAPI,
    value,
) -> None:
    """``get_patrols_minimal`` must forward ``patrols_overlap_daterange``."""
    captured: dict = {}

    @asynccontextmanager
    async def _mock_httpx_client(self):
        async with AsyncClient(
            transport=ASGITransport(app),
            base_url="http://test",
        ) as mock_httpx_client:
            original_stream = mock_httpx_client.stream

            def _capturing_stream(method, url, **kwargs):
                captured["method"] = method
                captured["url"] = url
                captured["query_params"] = kwargs.get("params")
                captured["body"] = kwargs.get("json")
                return original_stream(method, url, **kwargs)

            mock_httpx_client.stream = _capturing_stream  # type: ignore[assignment]
            yield mock_httpx_client

    with patch.object(
        ERWarehouseClient,
        "_httpx_client",
        _mock_httpx_client,
    ):
        er_client = ERWarehouseClient(
            server="some-site.pamdas.org",
            token="abc",
            warehouse_base_url="http://test",
        )
        er_client.get_patrols_minimal(
            since="2015-01-01T12:00:00",
            until="2015-03-01T12:00:00",
            patrol_type_value=["routine_patrol"],
            status=["done"],
            patrols_overlap_daterange=value,
        )

    params = _sent(captured)
    assert params["patrols_overlap_daterange"] == value


def test_client_get_patrols_minimal_default_patrols_overlap_daterange_is_true(
    app: FastAPI,
) -> None:
    """When the caller omits ``patrols_overlap_daterange`` it should default to True."""
    captured: dict = {}

    @asynccontextmanager
    async def _mock_httpx_client(self):
        async with AsyncClient(
            transport=ASGITransport(app),
            base_url="http://test",
        ) as mock_httpx_client:
            original_stream = mock_httpx_client.stream

            def _capturing_stream(method, url, **kwargs):
                captured["method"] = method
                captured["url"] = url
                captured["query_params"] = kwargs.get("params")
                captured["body"] = kwargs.get("json")
                return original_stream(method, url, **kwargs)

            mock_httpx_client.stream = _capturing_stream  # type: ignore[assignment]
            yield mock_httpx_client

    with patch.object(
        ERWarehouseClient,
        "_httpx_client",
        _mock_httpx_client,
    ):
        er_client = ERWarehouseClient(
            server="some-site.pamdas.org",
            token="abc",
            warehouse_base_url="http://test",
        )
        er_client.get_patrols_minimal(
            since="2015-01-01T12:00:00",
            until="2015-03-01T12:00:00",
            patrol_type_value=["routine_patrol"],
            status=["done"],
        )

    params = _sent(captured)
    assert params["patrols_overlap_daterange"] is True


# -------------------------------------------------------------------------
# POST /search transport tests (ERDW-269)
# -------------------------------------------------------------------------


@contextmanager
def _capture_request(app: FastAPI, captured: dict):
    """Patch the client's httpx factory and record what goes on the wire."""

    @asynccontextmanager
    async def _mock_httpx_client(self):
        async with AsyncClient(
            transport=ASGITransport(app),
            base_url="http://test",
        ) as mock_httpx_client:
            original_stream = mock_httpx_client.stream

            def _capturing_stream(method, url, **kwargs):
                captured["method"] = method
                captured["url"] = url
                captured["query_params"] = kwargs.get("params")
                captured["body"] = kwargs.get("json")
                return original_stream(method, url, **kwargs)

            mock_httpx_client.stream = _capturing_stream  # type: ignore[assignment]
            yield mock_httpx_client

    with patch.object(ERWarehouseClient, "_httpx_client", _mock_httpx_client):
        yield


def _client() -> ERWarehouseClient:
    return ERWarehouseClient(
        server="some-site.pamdas.org",
        token="abc",
        warehouse_base_url="http://test",
    )


def test_observations_filters_travel_in_the_body_not_the_url(app: FastAPI) -> None:
    """Filters must not reach the query string, or the URL-length bug returns."""
    captured: dict = {}
    with _capture_request(app, captured):
        _client().get_subjectgroup_observations(
            subject_group_name="Ecoscope",
            since="2015-01-01T12:00:00",
            until="2015-03-01T12:00:00",
        )

    assert captured["method"] == "POST"
    assert captured["url"] == "/observations/search/stream/arrow"
    body = captured["body"]
    assert body["subject_group_name"] == "Ecoscope"
    assert body["range_start"] == "2015-01-01T12:00:00"
    assert body["tenant_domain"] == "some-site.pamdas.org"
    # Response shaping stays in the query string; filters must not appear there.
    assert captured["query_params"] == {"store_type": "auto"}


def test_patrols_filters_travel_in_the_body_not_the_url(app: FastAPI) -> None:
    captured: dict = {}
    with _capture_request(app, captured):
        _client().get_patrols_minimal(
            since="2015-01-01T12:00:00",
            until="2015-03-01T12:00:00",
            patrol_type_value=["routine_patrol"],
            status=["done"],
        )

    assert captured["method"] == "POST"
    assert captured["url"] == "/patrols/search/stream/arrow"
    body = captured["body"]
    assert body["patrol_type_value"] == ["routine_patrol"]
    assert body["patrol_status"] == ["done"]
    assert captured["query_params"] == {"store_type": "auto"}


def test_events_filters_travel_in_the_body_not_the_url(app: FastAPI) -> None:
    captured: dict = {}
    with _capture_request(app, captured):
        _client().get_events(
            since="2015-01-01T12:00:00",
            until="2015-03-01T12:00:00",
            event_type=["wildlife_sighting"],
        )

    assert captured["method"] == "POST"
    assert captured["url"] == "/events/search/stream/arrow"
    assert captured["body"]["event_type"] == ["wildlife_sighting"]
    assert captured["query_params"] == {"store_type": "auto"}


def test_patrol_ids_list_too_long_for_a_url_is_sent_and_applied(
    app: FastAPI,
) -> None:
    """The regression this change exists for.

    ~1365 patrol UUIDs used to overflow httpx's 65536-char URL component limit
    and raise InvalidURL before the request left the process. In a JSON body
    there is no such ceiling, and the whole list must arrive intact -- a
    silently dropped patrol filter would return every patrol in the tenant.
    """
    import uuid

    patrol_ids = [str(uuid.uuid4()) for _ in range(2000)]
    captured: dict = {}
    with _capture_request(app, captured):
        table = _client().get_patrol_observations(
            patrols_df=pa.table({"id": pa.array(patrol_ids, type=pa.string())}),
        )

    assert isinstance(table, pa.Table)
    assert captured["method"] == "POST"
    assert sorted(captured["body"]["patrol_ids"]) == sorted(patrol_ids)
    # The same list as a query string would exceed httpx's limit outright.
    with pytest.raises(httpx.InvalidURL):
        httpx.Request(
            "GET",
            "http://test/observations/stream/arrow",
            params={"patrol_ids": patrol_ids},
        )
