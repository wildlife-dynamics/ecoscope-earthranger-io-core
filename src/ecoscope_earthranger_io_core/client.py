import asyncio
import base64
import io
import json
import time
from contextlib import asynccontextmanager
from datetime import datetime
from functools import cached_property
from typing import Any
from urllib.parse import urlparse

import httpx
import pyarrow as pa
from pydantic import BaseModel, PrivateAttr, SecretStr, field_validator

from ecoscope_earthranger_io_core.query import (
    ObservationsQuery,
    PatrolsQuery,
    QueryEngine,
)


async def _get_table(
    client: httpx.AsyncClient,
    route: str,
    query: BaseModel,
    headers: dict[str, str] | None = None,
    store_type: QueryEngine | None = None,
) -> pa.Table:
    """Fetch Arrow IPC stream from the warehouse API and return as a PyArrow Table.

    Args:
        client: The httpx async client.
        route: The API route to call.
        query: A Pydantic model specifying query parameters.
        headers: Optional headers to include.
        store_type: Optional store type to pass as a query parameter
            (maps to the DWH API's ``store_type`` param).
    """
    params = query.model_dump(exclude_none=True)
    if store_type is not None:
        params["store_type"] = store_type
    async with client.stream(
        "GET",
        route,
        params=params,
        headers=headers,
        timeout=600,
    ) as response:
        response.raise_for_status()
        sink = io.BytesIO()
        async for chunk in response.aiter_bytes():
            sink.write(chunk)
        sink.seek(0)
    source = sink.getvalue()
    if not source:
        raise ConnectionError(
            f"Warehouse API stream broke for {route}: "
            "received an empty response. The API may have crashed or "
            "the connection was closed unexpectedly."
        )
    table = pa.ipc.open_stream(source).read_all()
    return table


class ERWarehouseClient(BaseModel):
    """EarthRanger Warehouse Client.

    A client for fetching observations data from the EarthRanger Data Warehouse API.
    Implements the EarthRangerClientProtocol interface for use as a drop-in replacement
    for EarthRangerIO / EarthRangerClient in ecoscope-workflows.

    The warehouse API URL is resolved automatically from the EarthRanger status
    endpoint (``GET https://{server}/api/v1.0/status``), or can be overridden
    explicitly via ``warehouse_base_url``. Requests to the warehouse API are
    authenticated with both the EarthRanger API token and a Google Cloud ID token
    obtained via Application Default Credentials.

    Example:
        >>> from pydantic import SecretStr
        >>> client = ERWarehouseClient(
        ...     server="mep-dev.pamdas.org",
        ...     token=SecretStr("your-api-token"),
        ... )
        >>> table = client.get_subjectgroup_observations(  # doctest: +SKIP
        ...     subject_group_name="Elephants",
        ...     since="2024-01-01T00:00:00Z",
        ...     until="2024-01-31T23:59:59Z",
        ... )
        >>> table = client.get_patrol_observations_with_patrol_filter(  # doctest: +SKIP
        ...     since="2024-01-01T00:00:00Z",
        ...     until="2024-01-31T23:59:59Z",
        ...     patrol_type_value=["routine_patrol"],
        ...     status=["done"],
        ... )
    """

    # user-facing
    server: str  # tenant domain, e.g., "mep-dev.pamdas.org"
    username: str = ""
    password: SecretStr | None = None
    token: SecretStr | None = None

    # platform-level
    warehouse_base_url: str | None = None
    warehouse_observations_endpoint: str = "/observations"
    warehouse_patrols_endpoint: str = "/patrols"
    query_engine: QueryEngine = "auto"

    _resolved_base_url: str | None = PrivateAttr(default=None)
    _cached_id_token: SecretStr | None = PrivateAttr(default=None)
    _id_token_expiry: float = PrivateAttr(default=0.0)

    @field_validator("server", mode="before")
    @classmethod
    def _normalize_server(cls, v: str) -> str:
        if "://" in v:
            v = urlparse(v).hostname or v
        return v.strip("/")

    def _login(self) -> None:
        raise NotImplementedError(
            "Login not yet implemented, please pass `token` to constructor."
        )

    @cached_property
    def _token(self) -> SecretStr:
        if not self.token:
            raise NotImplementedError(
                "Login not yet implemented, please pass `token` to constructor."
            )
        return self.token

    async def _resolve_warehouse_url(self) -> str:
        """Resolve and cache the warehouse API base URL.

        If ``warehouse_base_url`` was provided at construction time it is
        returned immediately.  Otherwise the URL is fetched from the
        EarthRanger status endpoint and cached for the lifetime of this
        client instance.

        Returns:
            The warehouse API base URL.

        Raises:
            KeyError: If the status response is missing ``dwh_settings``
                or ``api_url``.
            ValueError: If ``api_url`` is present but empty.
            httpx.HTTPStatusError: If the status endpoint returns an error.
        """
        if self.warehouse_base_url:
            return self.warehouse_base_url
        if self._resolved_base_url:
            return self._resolved_base_url

        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"https://{self.server}/api/v1.0/status",
                timeout=30.0,
            )
            response.raise_for_status()
            data = response.json()

        try:
            api_url: str = data["data"]["dwh_settings"]["api_url"]
        except KeyError as exc:
            raise KeyError(
                "Status response from "
                f"https://{self.server}/api/v1.0/status "
                "is missing 'data.dwh_settings.api_url'"
            ) from exc

        if not api_url:
            raise ValueError(
                "Status response from "
                f"https://{self.server}/api/v1.0/status "
                "returned an empty 'data.dwh_settings.api_url'"
            )

        self._resolved_base_url = api_url
        return api_url

    def _get_id_token(self, audience: str) -> SecretStr:
        """Return a Google Cloud ID token for *audience*, with caching.

        Uses Application Default Credentials so it works transparently
        with user credentials locally (``gcloud auth application-default
        login``) and with service-account or metadata-server credentials
        in cloud environments.

        Args:
            audience: The target audience (the warehouse API base URL).

        Returns:
            A ``SecretStr``-wrapped Google ID token.
        """
        if self._cached_id_token and time.time() < self._id_token_expiry - 300:
            return self._cached_id_token

        from google.auth.transport.requests import (  # type: ignore[import-untyped]
            Request,
        )
        from google.oauth2 import id_token  # type: ignore[import-untyped]

        raw_token: str = id_token.fetch_id_token(Request(), audience)

        payload = raw_token.split(".")[1]
        padding = 4 - len(payload) % 4
        if padding != 4:
            payload += "=" * padding
        claims = json.loads(base64.urlsafe_b64decode(payload))
        self._id_token_expiry = float(claims.get("exp", 0))

        self._cached_id_token = SecretStr(raw_token)
        return self._cached_id_token

    @asynccontextmanager
    async def _httpx_client(self):
        base_url = await self._resolve_warehouse_url()
        async with httpx.AsyncClient(base_url=base_url) as client:
            yield client

    def _get_auth_headers(self) -> dict[str, str]:
        """Return authentication headers for API requests.

        Includes the EarthRanger API token and a Google Cloud ID token
        for authenticating to the private Cloud Run warehouse service.
        """
        base_url = self.warehouse_base_url or self._resolved_base_url
        if not base_url:
            raise RuntimeError(
                "Warehouse base URL has not been resolved yet. "
                "Ensure _httpx_client() is entered before calling "
                "_get_auth_headers()."
            )
        return {
            "X-EarthRanger-API-Token": self._token.get_secret_value(),
            "Authorization": f"Bearer {self._get_id_token(base_url).get_secret_value()}",
        }

    async def _fetch_observations_arrow(
        self,
        query: ObservationsQuery,
        query_engine: QueryEngine = "auto",
    ) -> pa.Table:
        """Internal async method to fetch observations as Arrow table."""
        async with self._httpx_client() as client:
            table = await _get_table(
                client=client,
                route=f"{self.warehouse_observations_endpoint}/stream/arrow",
                query=query,
                headers=self._get_auth_headers(),
                store_type=query_engine,
            )
        return table

    async def _fetch_patrols_arrow(
        self,
        query: PatrolsQuery,
        query_engine: QueryEngine = "auto",
    ) -> pa.Table:
        """Internal async method to fetch patrols as Arrow table."""
        async with self._httpx_client() as client:
            table = await _get_table(
                client=client,
                route=f"{self.warehouse_patrols_endpoint}/stream/arrow",
                query=query,
                headers=self._get_auth_headers(),
                store_type=query_engine,
            )
        return table

    def _run_async(self, coro):
        """Run an async coroutine synchronously.

        Handles the case where we're already inside an event loop (e.g., in tests)
        by using the existing loop instead of creating a new one.
        """
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            # No running loop, use asyncio.run()
            return asyncio.run(coro)

        # Already in a running loop - create a new thread to run the coroutine
        import concurrent.futures

        with concurrent.futures.ThreadPoolExecutor() as executor:
            future = executor.submit(asyncio.run, coro)
            return future.result()

    # -------------------------------------------------------------------------
    # EarthRangerClientProtocol implementation - Observations
    # -------------------------------------------------------------------------

    def get_subjectgroup_observations(
        self,
        subject_group_name: str,
        include_subject_details: bool = True,
        include_inactive: bool = True,
        include_details: bool = True,
        include_subjectsource_details: bool = False,
        since: str | None = None,
        until: str | None = None,
        query_engine: QueryEngine | None = None,
    ) -> pa.Table:
        """Get observations for a subject group from EarthRanger Data Warehouse.

        Args:
            subject_group_name: Name of the subject group to fetch observations for.
            include_subject_details: Ignored (for interface compatibility).
            include_inactive: Ignored (for interface compatibility).
            include_details: Ignored (for interface compatibility).
            include_subjectsource_details: Ignored (for interface compatibility).
            since: Start of time range (ISO 8601 format).
            until: End of time range (ISO 8601 format).
            query_engine: Backend engine to use. Defaults to the client-level
                setting (``self.query_engine``).

        Returns:
            PyArrow Table with observations data.
            Schema: OBSERVATIONS_SCHEMA__ECOSCOPE_SLIM_V1.
        """
        if since is None or until is None:
            raise ValueError("Both 'since' and 'until' must be provided")

        engine = query_engine or self.query_engine
        query = ObservationsQuery(
            tenant_domain=self.server,
            range_start=datetime.fromisoformat(since),
            range_end=datetime.fromisoformat(until),
            subject_group_name=subject_group_name,
        )
        table = self._run_async(
            self._fetch_observations_arrow(query, query_engine=engine)
        )
        return table

    def get_patrol_observations_with_patrol_filter(
        self,
        since: str | None = None,
        until: str | None = None,
        patrol_type_value: list[str] | None = None,
        status: list[str] | None = None,
        include_patrol_details: bool = True,
        sub_page_size: int | None = None,
        query_engine: QueryEngine | None = None,
    ) -> pa.Table:
        """Get patrol observations filtered by patrol type and status.

        Args:
            since: Start of time range (ISO 8601 format).
            until: End of time range (ISO 8601 format).
            patrol_type_value: List of patrol type values to filter by.
            status: List of patrol statuses to filter by (e.g., ["done"]).
            include_patrol_details: Whether to include patrol metadata.
            sub_page_size: Ignored (for interface compatibility).
            query_engine: Backend engine to use. Defaults to the client-level
                setting (``self.query_engine``).

        Returns:
            PyArrow Table with patrol observations data including patrol metadata.
            Schema: OBSERVATIONS_WITH_PATROL_SCHEMA_SLIM_V1.
        """
        if since is None or until is None:
            raise ValueError("Both 'since' and 'until' must be provided")

        engine = query_engine or self.query_engine
        query = ObservationsQuery(
            tenant_domain=self.server,
            range_start=datetime.fromisoformat(since),
            range_end=datetime.fromisoformat(until),
            patrol_type_value=patrol_type_value,
            patrol_status=status,  # type: ignore[arg-type]
            include_patrol_details=include_patrol_details,
        )
        table = self._run_async(
            self._fetch_observations_arrow(query, query_engine=engine)
        )
        return table

    def get_patrols_minimal(
        self,
        since: str,
        until: str,
        patrol_type_value: list[str] | None = None,
        status: list[str] | None = None,
        sub_page_size: int | None = None,
        query_engine: QueryEngine | None = None,
    ) -> pa.Table:
        """Get minimal patrol data from EarthRanger Data Warehouse.

        Note:
            This method returns minimal patrol data and does NOT include patrol
            events. Unlike the EarthRanger API's `get_patrols` method, this returns
            only patrol metadata, without segments or associated events.
            Please consider this method experimental and use it when event data is not required.

        Args:
            since: Start of time range (ISO 8601 format).
            until: End of time range (ISO 8601 format).
            patrol_type_value: List of patrol type values to filter by.
            status: List of patrol statuses to filter by (e.g., ["done"]).
            sub_page_size: Ignored (for interface compatibility).
            query_engine: Backend engine to use. Defaults to the client-level
                setting (``self.query_engine``).

        Returns:
            PyArrow Table with minimal patrol data (metadata only, no segments
            or events). Schema: PATROLS_ONLY_SCHEMA_V1.
        """
        engine = query_engine or self.query_engine
        query = PatrolsQuery(
            tenant_domain=self.server,
            range_start=datetime.fromisoformat(since),
            range_end=datetime.fromisoformat(until),
            patrol_type_value=patrol_type_value,
            patrol_status=status,
        )
        return self._run_async(self._fetch_patrols_arrow(query, query_engine=engine))

    def get_patrol_observations(
        self,
        patrols_df: Any,
        include_patrol_details: bool = True,
        sub_page_size: int | None = None,
        query_engine: QueryEngine | None = None,
    ) -> pa.Table:
        """Get observations for patrols from EarthRanger Data Warehouse.

        Args:
            patrols_df: PyArrow Table or Pandas DataFrame with patrol data
                (as returned by get_patrols_minimal).
            include_patrol_details: Whether to include patrol metadata.
            sub_page_size: Ignored (for interface compatibility).
            query_engine: Backend engine to use. Defaults to the client-level
                setting (``self.query_engine``).

        Returns:
            PyArrow Table with patrol observations data.
            Schema: OBSERVATIONS_WITH_PATROL_SCHEMA_SLIM_V1.
        """
        engine = query_engine or self.query_engine
        # Handle both PyArrow Table and Pandas DataFrame
        if hasattr(patrols_df, "column"):  # PyArrow Table
            patrol_ids = patrols_df.column("id").to_pylist()
        else:  # Pandas DataFrame
            patrol_ids = patrols_df["id"].tolist()

        query = ObservationsQuery(
            tenant_domain=self.server,
            patrol_ids=list(set(patrol_ids)),
            include_patrol_details=include_patrol_details,
        )
        return self._run_async(
            self._fetch_observations_arrow(query, query_engine=engine)
        )

    # -------------------------------------------------------------------------
    # EarthRangerClientProtocol implementation - Not Implemented
    # -------------------------------------------------------------------------

    def get_patrol_events(
        self,
        since: str | None = None,
        until: str | None = None,
        patrol_type_value: list[str] | None = None,
        event_type: list[str] | None = None,
        status: list[str] | None = None,
        drop_null_geometry: bool = False,
        sub_page_size: int | None = None,
    ) -> pa.Table:
        """Not implemented - events not yet supported by the Data Warehouse."""
        raise NotImplementedError(
            "get_patrol_events is not yet implemented in ERWarehouseClient. "
            "Events are not currently supported by the Data Warehouse API."
        )

    def get_events(
        self,
        since: str | None = None,
        until: str | None = None,
        event_type: list[str] | None = None,
        drop_null_geometry: bool = False,
        include_details: bool = False,
        include_updates: bool = False,
        include_related_events: bool = False,
    ) -> pa.Table:
        """Not implemented - events not yet supported by the Data Warehouse."""
        raise NotImplementedError(
            "get_events is not yet implemented in ERWarehouseClient. "
            "Events are not currently supported by the Data Warehouse API."
        )

    def get_event_types(self) -> Any:
        """Not implemented - events not yet supported by the Data Warehouse."""
        raise NotImplementedError(
            "get_event_types is not yet implemented in ERWarehouseClient. "
            "Events are not currently supported by the Data Warehouse API."
        )

    def get_event_type_display_names_from_events(
        self,
        events_gdf: Any,
        append_category_names: str = "duplicates",
    ) -> Any:
        """Not implemented - events not yet supported by the Data Warehouse."""
        raise NotImplementedError(
            "get_event_type_display_names_from_events is not yet implemented in ERWarehouseClient. "
            "Events are not currently supported by the Data Warehouse API."
        )
