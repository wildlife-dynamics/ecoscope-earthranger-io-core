from contextlib import asynccontextmanager
from datetime import datetime
from typing import AsyncIterable, Callable
from unittest.mock import AsyncMock, MagicMock, patch

from pydantic import SecretStr

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

        original_get_table = _get_table

        async def _capturing_get_table(*args, **kwargs):
            captured_params["store_type"] = kwargs.get("store_type")
            return await original_get_table(*args, **kwargs)

        with patch(
            "ecoscope_earthranger_io_core.client._get_table",
            side_effect=_capturing_get_table,
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

        original_get_table = _get_table

        async def _capturing_get_table(*args, **kwargs):
            captured_params["store_type"] = kwargs.get("store_type")
            return await original_get_table(*args, **kwargs)

        with patch(
            "ecoscope_earthranger_io_core.client._get_table",
            side_effect=_capturing_get_table,
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

        original_get_table = _get_table

        async def _capturing_get_table(*args, **kwargs):
            captured_params["store_type"] = kwargs.get("store_type")
            return await original_get_table(*args, **kwargs)

        with patch(
            "ecoscope_earthranger_io_core.client._get_table",
            side_effect=_capturing_get_table,
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

        original_get_table = _get_table

        async def _capturing_get_table(*args, **kwargs):
            captured_params["store_type"] = kwargs.get("store_type")
            return await original_get_table(*args, **kwargs)

        with patch(
            "ecoscope_earthranger_io_core.client._get_table",
            side_effect=_capturing_get_table,
        ):
            table = er_client.get_subjectgroup_observations(
                subject_group_name="Ecoscope",
                since="2015-01-01T12:00:00",
                until="2015-03-01T12:00:00",
                query_engine="iceberg-bq",
            )
        assert isinstance(table, pa.Table)
        assert captured_params["store_type"] == "iceberg-bq"


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
