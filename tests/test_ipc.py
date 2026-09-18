import inspect
import io
import json
from contextlib import asynccontextmanager, contextmanager
from datetime import datetime
from typing import AsyncIterable, Callable
from unittest.mock import AsyncMock, MagicMock, patch

import httpx

from pydantic import SecretStr

import pyarrow as pa
import pytest
from fastapi import FastAPI, Response
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


@pytest.mark.asyncio
async def test__search_table_raises_on_empty_stream() -> None:
    """Same guard as the GET helper, on the path that now serves the reads.

    All three Arrow endpoints go through ``_search_table``; ``_get_table``
    only serves ``/event_types``. An empty body must surface as a clear
    ConnectionError rather than a pyarrow parse error.
    """
    query = ObservationsQuery(
        tenant_domain="some-site.pamdas.org",
        subject_ids=["subject1"],
        range_start=datetime(2023, 1, 1),
        range_end=datetime(2023, 12, 31),
    )

    async def empty_response(scope, receive, send):
        assert scope["method"] == "POST"
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b""})

    async with AsyncClient(
        transport=ASGITransport(empty_response),
        base_url="http://test",
    ) as client:
        with pytest.raises(ConnectionError, match="stream broke"):
            await _search_table(
                client=client,
                route="/observations/search/stream/arrow",
                query=query,
            )


@pytest.mark.asyncio
async def test__search_table_returns_table_and_sends_filters_in_the_body() -> None:
    """Success path for the helper, plus proof the body is what carries filters."""
    seen: dict = {}

    async def echo_app(scope, receive, send):
        body = b""
        while True:
            message = await receive()
            body += message.get("body", b"")
            if not message.get("more_body"):
                break
        seen["method"] = scope["method"]
        seen["path"] = scope["path"]
        seen["query_string"] = scope["query_string"].decode()
        seen["body"] = json.loads(body)

        sink = io.BytesIO()
        table = pa.table({"a": pa.array([1, 2, 3], type=pa.int64())})
        with pa.ipc.new_stream(sink, table.schema) as writer:
            writer.write_table(table)
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": sink.getvalue()})

    query = ObservationsQuery(
        tenant_domain="some-site.pamdas.org",
        subject_ids=["subject1"],
        range_start=datetime(2023, 1, 1),
        range_end=datetime(2023, 12, 31),
    )

    async with AsyncClient(
        transport=ASGITransport(echo_app),
        base_url="http://test",
    ) as client:
        table = await _search_table(
            client=client,
            route="/observations/search/stream/arrow",
            query=query,
            store_type="iceberg-dd",
        )

    assert isinstance(table, pa.Table)
    assert table.num_rows == 3
    assert seen["method"] == "POST"
    assert seen["path"] == "/observations/search/stream/arrow"
    assert seen["body"]["subject_ids"] == ["subject1"]
    assert seen["body"]["range_start"] == "2023-01-01T00:00:00"
    # Only response shaping belongs in the URL.
    assert seen["query_string"] == "store_type=iceberg-dd"


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
    patrol segment.

    The warehouse types the detail columns unless raw_details is set, so a
    default call matches PATROLS_WITH_EVENTS_NESTED_SCHEMA_V1 everywhere except
    those two fields; raw mode matches it exactly (asserted below).
    """

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
    assert table.num_rows > 0
    assert "patrol_segments" in table.column_names
    segment_type = table.schema.field("patrol_segments").type.value_type
    assert pa.types.is_struct(segment_type.field("segment_details").type)
    assert [f.name for f in segment_type] == [
        f.name
        for f in PATROLS_WITH_EVENTS_NESTED_SCHEMA_V1.field(
            "patrol_segments"
        ).type.value_type
    ]

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
    assert captured["body"]["raw_details"] is True
    # raw details are still details, so the payload is included
    assert captured["body"]["include_details"] is True


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
    assert captured["body"]["include_details"] is False
    assert captured["body"]["raw_details"] is False


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
    assert captured["body"]["include_details"] is True
    assert captured["body"]["raw_details"] is False

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
    assert captured["body"]["raw_details"] is False
    assert captured["body"]["parse_detail_datetimes"] is True
    assert captured["body"]["invalid_only"] is True


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
    assert "range_start" not in captured["body"]
    assert "range_end" not in captured["body"]

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
    assert "range_start" in captured["body"]
    assert "range_end" not in captured["body"]


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
    assert captured["body"]["invalid_details"] == "coerce"


def test_client_get_events_forwards_state(app: FastAPI) -> None:
    """A state filter threads through into the EventsQuery body."""
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
            event_type=["wildlife_sighting"],
            state=["active", "resolved"],
        )
    assert captured["body"]["state"] == ["active", "resolved"]


def test_client_get_events_empty_state_collapses_to_no_filter(app: FastAPI) -> None:
    """state=[] means "no filter": it collapses to None and is dropped from the
    body (parity with event_type), rather than filtering to zero states."""
    captured: dict = {}
    with patch.object(
        ERWarehouseClient, "_httpx_client", _capturing_mock_httpx_client(app, captured)
    ):
        er_client = ERWarehouseClient(
            server="some-site.pamdas.org",
            token="abc",
            warehouse_base_url="http://test",
        )
        er_client.get_events(event_type=["wildlife_sighting"], state=[])
    assert "state" not in captured["body"]


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
    assert captured["body"]["raw_details"] is True
    assert captured["body"]["include_details"] is True


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
    assert captured["body"]["invalid_details"] == "drop"
    assert captured["body"]["raw_details"] is False
    assert captured["body"]["include_details"] is True


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

    body = captured["body"]
    if value is None:
        assert "exclusion_flags" not in body
    else:
        assert body["exclusion_flags"] == value


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

    body = captured["body"]
    if value is None:
        assert "exclusion_flags" not in body
    else:
        assert body["exclusion_flags"] == value


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

    body = captured["body"]
    assert body["patrols_overlap_daterange"] == value


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

    body = captured["body"]
    assert body["patrols_overlap_daterange"] is True


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

    body = captured["body"]
    assert body["patrols_overlap_daterange"] == value


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

    body = captured["body"]
    assert body["patrols_overlap_daterange"] is True


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


def test_empty_patrols_df_sends_no_patrol_ids(app: FastAPI) -> None:
    """An empty patrols_df must omit patrol_ids, not send an empty list.

    A query string dropped empty-list params outright; a JSON body carries
    ``[]`` through. A server that did not normalize ``[]`` back to ``None``
    would read it as "no patrol filter" and scan the whole tenant.
    """
    captured: dict = {}
    with _capture_request(app, captured):
        _client().get_patrol_observations(
            patrols_df=pa.table({"id": pa.array([], type=pa.string())}),
        )

    assert "patrol_ids" not in captured["body"]


# -------------------------------------------------------------------------
# Patrol type / segment schema discovery
# -------------------------------------------------------------------------


def test_client_get_patrol_types(app: FastAPI) -> None:
    """get_patrol_types returns the value -> display mapping table."""
    from ecoscope_earthranger_io_core.arrow import PATROL_TYPES_SCHEMA_V1

    with patch.object(
        ERWarehouseClient, "_httpx_client", _events_mock_httpx_client(app)
    ):
        table = _client().get_patrol_types()

    assert isinstance(table, pa.Table)
    assert table.schema.equals(PATROL_TYPES_SCHEMA_V1)
    assert dict(
        zip(table.column("value").to_pylist(), table.column("display").to_pylist())
    ) == {"routine_patrol": "Routine Patrol", "aerial_patrol": "Aerial Patrol"}


def test_client_get_patrol_schema_arrow(app: FastAPI) -> None:
    """format="arrow" reads a bare Arrow schema message, not an IPC stream."""
    with patch.object(
        ERWarehouseClient, "_httpx_client", _events_mock_httpx_client(app)
    ):
        schema = _client().get_patrol_schema("routine_patrol")

    assert isinstance(schema, pa.Schema)
    details = schema.field("type_details").type
    assert pa.types.is_struct(details)
    assert [f.name for f in details] == ["vehicle", "departed_at"]
    assert details.field("departed_at").type == pa.string()


def test_client_get_patrol_schema_parse_detail_datetimes(app: FastAPI) -> None:
    """The datetime opt-in types date-time leaves as timestamps."""
    with patch.object(
        ERWarehouseClient, "_httpx_client", _events_mock_httpx_client(app)
    ):
        schema = _client().get_patrol_schema(
            "routine_patrol", parse_detail_datetimes=True
        )

    details = schema.field("type_details").type
    assert details.field("departed_at").type == pa.timestamp("ns", tz="UTC")


def test_client_get_patrol_schema_json(app: FastAPI) -> None:
    """format="json" returns the informational {field: type_str} mapping."""
    with patch.object(
        ERWarehouseClient, "_httpx_client", _events_mock_httpx_client(app)
    ):
        mapping = _client().get_patrol_schema("routine_patrol", format="json")

    assert mapping == {"vehicle": "string", "departed_at": "string"}


def test_client_get_patrol_schema_unknown_type_raises_value_error(
    app: FastAPI,
) -> None:
    """An unknown patrol type surfaces the API's 404 detail, not a bare status."""
    with patch.object(
        ERWarehouseClient, "_httpx_client", _events_mock_httpx_client(app)
    ):
        with pytest.raises(ValueError, match="no_such_type"):
            _client().get_patrol_schema("no_such_type")


def test_client_get_segment_schema(app: FastAPI) -> None:
    """get_segment_schema takes no key: there is one per tenant."""
    with patch.object(
        ERWarehouseClient, "_httpx_client", _events_mock_httpx_client(app)
    ):
        schema = _client().get_segment_schema()
        mapping = _client().get_segment_schema(format="json")

    details = schema.field("segment_details").type
    assert [f.name for f in details] == ["weather", "briefed_at"]
    assert mapping == {"weather": "string", "briefed_at": "string"}


# -------------------------------------------------------------------------
# Typed details on the patrol getters
# -------------------------------------------------------------------------


def test_patrol_detail_args_reach_the_query(app: FastAPI) -> None:
    """include_pauses / raw_details / parse_detail_datetimes reach PatrolsQuery."""
    captured: dict = {}
    with _capture_request(app, captured):
        _client().get_patrols(
            since="2015-01-01T12:00:00",
            until="2015-03-01T12:00:00",
            include_pauses=True,
            raw_details=True,
        )

    body = captured["body"]
    assert body["include_pauses"] is True
    assert body["raw_details"] is True
    assert body["parse_detail_datetimes"] is False


def test_patrols_minimal_takes_include_pauses_only(app: FastAPI) -> None:
    """A patrols-only response carries no detail columns to shape.

    include_pauses is a store-level predicate and still applies; the three
    detail arguments would be inert, so get_patrols_minimal does not take them.
    """
    captured: dict = {}
    with _capture_request(app, captured):
        _client().get_patrols_minimal(
            since="2015-01-01T12:00:00",
            until="2015-03-01T12:00:00",
            include_pauses=True,
        )

    assert captured["body"]["include_pauses"] is True

    takes = inspect.signature(ERWarehouseClient.get_patrols_minimal).parameters
    assert "include_pauses" in takes
    assert not {"raw_details", "parse_detail_datetimes", "typed_type_details"} & set(
        takes
    )


def test_patrol_getters_default_to_todays_behaviour(app: FastAPI) -> None:
    """A caller passing none of the new arguments sends the old defaults."""
    captured: dict = {}
    with _capture_request(app, captured):
        _client().get_patrols(since="2015-01-01T12:00:00", until="2015-03-01T12:00:00")

    body = captured["body"]
    assert body["include_pauses"] is False
    assert body["raw_details"] is False
    assert body["parse_detail_datetimes"] is False


def test_typed_type_details_requires_exactly_one_patrol_type() -> None:
    """The one-type rule is validated client-side, naming the way out."""
    for patrol_type_value in (None, [], ["a", "b"]):
        with pytest.raises(ValueError, match="exactly one patrol_type_value"):
            _client().get_patrols(
                patrol_type_value=patrol_type_value, typed_type_details=True
            )


def test_typed_type_details_accepts_one_patrol_type(app: FastAPI) -> None:
    """Exactly one patrol type passes validation and reaches the request."""
    captured: dict = {}
    with _capture_request(app, captured):
        _client().get_patrols(
            patrol_type_value=["routine_patrol"],
            typed_type_details=True,
            parse_detail_datetimes=True,
        )

    assert captured["body"]["patrol_type_value"] == ["routine_patrol"]


def test_typed_segment_details_is_not_gated_on_patrol_type_count(
    app: FastAPI,
) -> None:
    """segment_details stays typed across several patrol types.

    The one-type rule belongs to type_details alone; applying it here would
    make the common multi-type query untypeable for no reason.
    """
    with patch.object(
        ERWarehouseClient, "_httpx_client", _events_mock_httpx_client(app)
    ):
        table = _client().get_patrol_events(
            patrol_type_value=["routine_patrol", "aerial_patrol"],
            parse_detail_datetimes=True,
        )

    assert "segment__weather" in table.column_names
    assert table.schema.field("segment__briefed_at").type == pa.timestamp(
        "ns", tz="UTC"
    )
    # Several types, so type_details is served as JSON text rather than refused.
    assert "type_details" in table.column_names
    assert table.column("type_details").to_pylist() == ['{"vehicle": "landcruiser"}']


def test_raw_details_cannot_be_combined_with_parse_detail_datetimes() -> None:
    """The one combination the warehouse rejects is caught before the request."""
    with pytest.raises(ValueError, match="parse_detail_datetimes"):
        _client().get_patrols(raw_details=True, parse_detail_datetimes=True)


def test_typed_type_details_cannot_be_combined_with_raw_details() -> None:
    """Asking for a typed struct while opting out of typing is a contradiction."""
    with pytest.raises(ValueError, match="raw_details"):
        _client().get_patrols(
            patrol_type_value=["routine_patrol"],
            typed_type_details=True,
            raw_details=True,
        )


def test_patrol_events_flattens_details_with_prefixes(app: FastAPI) -> None:
    """Typed detail fields flatten to segment__<key> / type__<key> columns."""
    with patch.object(
        ERWarehouseClient, "_httpx_client", _events_mock_httpx_client(app)
    ):
        table = _client().get_patrol_events(
            patrol_type_value=["routine_patrol"],
            parse_detail_datetimes=True,
            typed_type_details=True,
        )

    assert {"segment__weather", "segment__briefed_at", "type__vehicle"} <= set(
        table.column_names
    )
    # The string columns they replace are gone, not carried alongside.
    assert "segment_details" not in table.column_names
    assert "type_details" not in table.column_names
    row = table.to_pylist()[0]
    assert row["segment__weather"] == "clear"
    assert row["type__vehicle"] == "landcruiser"
    # Event and patrol context survive the expansion unchanged.
    assert row["event_type"] == "wildlife_sighting"
    assert row["patrol_id"] == "patrol1"


def test_patrol_events_raw_mode_matches_the_published_flat_schema(
    app: FastAPI,
) -> None:
    """raw_details is the stable shape: exactly PATROL_EVENTS_FLAT_SCHEMA_V1.

    Typed mode expands details into per-field columns, so its column set
    follows the tenant's schema documents. Raw mode does not.
    """
    from ecoscope_earthranger_io_core.arrow import PATROL_EVENTS_FLAT_SCHEMA_V1

    with patch.object(
        ERWarehouseClient, "_httpx_client", _events_mock_httpx_client(app)
    ):
        table = _client().get_patrol_events(raw_details=True)

    assert table.schema.equals(PATROL_EVENTS_FLAT_SCHEMA_V1)
    assert table.to_pylist()[0]["segment_details"] == '{"weather": "clear"}'


def test_patrol_events_falls_back_to_json_text_without_a_schema(
    app: FastAPI,
) -> None:
    """A tenant that authored no schemas keeps the published flat schema.

    The API declines to type an empty struct, because doing so would drop the
    column; the client must follow rather than expand it away.
    """
    from ecoscope_earthranger_io_core.arrow import PATROL_EVENTS_FLAT_SCHEMA_V1

    with patch.object(
        ERWarehouseClient, "_httpx_client", _events_mock_httpx_client(app)
    ):
        table = ERWarehouseClient(
            server="no-schemas.pamdas.org",
            token="abc",
            warehouse_base_url="http://test",
        ).get_patrol_events()

    assert table.schema.equals(PATROL_EVENTS_FLAT_SCHEMA_V1)


def test_typed_type_details_rejects_an_untyped_response(app: FastAPI) -> None:
    """Naming one patrol type is necessary but not sufficient.

    A known type whose schema defines no fields is served as JSON text, so the
    flag has to check what came back, not just what was asked for.
    """
    with patch.object(
        ERWarehouseClient, "_httpx_client", _events_mock_httpx_client(app)
    ):
        with pytest.raises(ValueError, match="served"):
            ERWarehouseClient(
                server="no-schemas.pamdas.org",
                token="abc",
                warehouse_base_url="http://test",
            ).get_patrols(patrol_type_value=["routine_patrol"], typed_type_details=True)


def test_client_get_segment_schema_parse_detail_datetimes(app: FastAPI) -> None:
    """The datetime opt-in reaches the segment schema endpoint too."""
    with patch.object(
        ERWarehouseClient, "_httpx_client", _events_mock_httpx_client(app)
    ):
        schema = _client().get_segment_schema(parse_detail_datetimes=True)

    details = schema.field("segment_details").type
    assert details.field("briefed_at").type == pa.timestamp("ns", tz="UTC")


def test_patrol_types_threads_the_query_engine(app: FastAPI) -> None:
    """query_engine reaches the listing request as store_type."""
    captured: dict = {}

    @asynccontextmanager
    async def _mock_httpx_client(self):
        async with AsyncClient(
            transport=ASGITransport(app), base_url="http://test"
        ) as client:
            original = client.stream

            def _stream(method, url, **kwargs):
                captured["params"] = kwargs.get("params")
                return original(method, url, **kwargs)

            client.stream = _stream  # type: ignore[assignment]
            yield client

    with patch.object(ERWarehouseClient, "_httpx_client", _mock_httpx_client):
        _client().get_patrol_types(query_engine="iceberg-dd")

    assert captured["params"]["store_type"] == "iceberg-dd"


def test_patrol_schema_404_without_a_json_body_still_names_the_type() -> None:
    """A 404 whose body is not JSON falls back to the client's own message."""
    app = FastAPI()

    @app.get("/patrols/schema")
    async def _schema():
        return Response(content="upstream down", status_code=404)

    with patch.object(
        ERWarehouseClient, "_httpx_client", _events_mock_httpx_client(app)
    ):
        with pytest.raises(ValueError, match="routine_patrol"):
            _client().get_patrol_schema("routine_patrol")


def test_typed_type_details_rejects_a_typed_segment_but_untyped_type(
    app: FastAPI,
) -> None:
    """The post-check must read type_details, not segment_details.

    A tenant whose site-wide segment schema has fields but whose patrol types'
    schemas do not is the case that separates them: segment_details comes back
    typed while type_details is JSON text, so a check that looked at the wrong
    column would wrongly pass.
    """
    client = ERWarehouseClient(
        server="no-type-schema.pamdas.org",
        token="abc",
        warehouse_base_url="http://test",
    )
    with patch.object(
        ERWarehouseClient, "_httpx_client", _events_mock_httpx_client(app)
    ):
        # segment_details is typed here, so the flattening still expands it.
        table = client.get_patrol_events(patrol_type_value=["routine_patrol"])
        assert "segment__weather" in table.column_names
        assert "type_details" in table.column_names

        with pytest.raises(ValueError, match="type_details"):
            client.get_patrols(
                patrol_type_value=["routine_patrol"], typed_type_details=True
            )


def test_get_patrols_raw_mode_matches_the_published_nested_schema(
    app: FastAPI,
) -> None:
    """raw_details is the stable nested shape: PATROLS_WITH_EVENTS_NESTED_SCHEMA_V1.

    The default is typed, so only raw mode can be pinned to the published
    schema — without this, drift in the raw path would go unnoticed.
    """
    with patch.object(
        ERWarehouseClient, "_httpx_client", _events_mock_httpx_client(app)
    ):
        table = _client().get_patrols(
            since="2015-01-01T12:00:00",
            until="2015-03-01T12:00:00",
            raw_details=True,
        )

    assert table.schema.equals(PATROLS_WITH_EVENTS_NESTED_SCHEMA_V1)
