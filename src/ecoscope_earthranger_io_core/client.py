import asyncio
import base64
import io
import json
import time
from contextlib import asynccontextmanager
from datetime import datetime
from functools import cached_property
from typing import Any, Literal, overload
from urllib.parse import urlparse

import httpx
import pyarrow as pa
from pydantic import BaseModel, PrivateAttr, SecretStr, field_validator

from ecoscope_earthranger_io_core.arrow import PATROL_EVENTS_FLAT_SCHEMA_V1
from ecoscope_earthranger_io_core.query import (
    EventsQuery,
    EventTypeSchemaQuery,
    EventTypesQuery,
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
    extra_params: dict | None = None,
) -> pa.Table:
    """Fetch Arrow IPC stream from the warehouse API and return as a PyArrow Table.

    Args:
        client: The httpx async client.
        route: The API route to call.
        query: A Pydantic model specifying query parameters.
        headers: Optional headers to include.
        store_type: Optional store type to pass as a query parameter
            (maps to the DWH API's ``store_type`` param).
        extra_params: Optional response-shaping flags merged into the query
            string after the query model fields and ``store_type``.
    """
    params = query.model_dump(exclude_none=True)
    if store_type is not None:
        params["store_type"] = store_type
    if extra_params:
        params.update(extra_params)
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


async def _search_table(
    client: httpx.AsyncClient,
    route: str,
    query: BaseModel,
    headers: dict[str, str] | None = None,
    store_type: QueryEngine | None = None,
    extra_params: dict | None = None,
) -> pa.Table:
    """Read an Arrow IPC stream from a warehouse ``/search`` route.

    Like :func:`_get_table`, this fetches data and returns it as a PyArrow
    Table -- despite issuing a POST, nothing is created or mutated. The
    warehouse exposes these reads as POST because the filters travel in a JSON
    body rather than the query string, and a GET body is not routable.

    Sending filters in the body means the ones that grow with the result set
    (``patrol_ids``, ``subject_ids``) are no longer bounded by URL length:
    httpx refuses to build a request whose query component exceeds 65536
    chars, and the load balancer caps the request line well below that --
    around 1365 patrol UUIDs the request used to fail client-side, before it
    ever reached the API.

    Response-shaping options (``store_type`` and anything in ``extra_params``)
    stay in the query string, matching the API's ``/search`` routes.

    Args:
        client: The httpx async client.
        route: The API route to call.
        query: A Pydantic model specifying the query filters (sent as the body).
        headers: Optional headers to include.
        store_type: Optional store type to pass as a query parameter
            (maps to the DWH API's ``store_type`` param).
        extra_params: Optional response-shaping flags merged into the query
            string after ``store_type``.
    """
    params: dict = {}
    if store_type is not None:
        params["store_type"] = store_type
    if extra_params:
        params.update(extra_params)
    # mode="json" so datetime fields serialize to ISO strings; the plain dump
    # leaves datetime objects that json= cannot encode.
    body = query.model_dump(mode="json", exclude_none=True)
    async with client.stream(
        "POST",
        route,
        json=body,
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
    warehouse_events_endpoint: str = "/events"
    warehouse_event_types_endpoint: str = "/event_types"
    query_engine: QueryEngine = "auto"

    _resolved_base_url: str | None = PrivateAttr(default=None)
    _cached_id_token: SecretStr | None = PrivateAttr(default=None)
    _id_token_expiry: float = PrivateAttr(default=0.0)

    @field_validator("server", mode="before")
    @classmethod
    def _normalize_server(cls, v: str) -> str:
        """Normalize ``server`` to ``host[:port]``.

        Strips scheme, path, query, and fragment so that downstream
        interpolation into ``https://{server}/api/v1.0/...`` always yields a
        well-formed URL, regardless of how the caller supplied the value.
        """
        if not isinstance(v, str) or not v.strip():
            raise ValueError("server must be a non-empty string")
        candidate = v.strip()
        # `urlparse` only populates netloc/hostname when a scheme is present;
        # prepend a protocol-relative marker so schemeless inputs (with or
        # without a path) are still parsed into host/port components.
        if "://" not in candidate:
            candidate = f"//{candidate}"
        parsed = urlparse(candidate)
        host = parsed.hostname
        if not host:
            raise ValueError(f"Invalid server: {v!r}")
        return f"{host}:{parsed.port}" if parsed.port else host

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
            audience: The target audience (the warehouse API domain).

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
        audience = urlparse(base_url).hostname
        if not audience:
            raise ValueError(
                f"Could not extract a valid hostname from warehouse URL: {base_url!r}"
            )
        return {
            "X-EarthRanger-API-Token": self._token.get_secret_value(),
            "Authorization": f"Bearer {self._get_id_token(audience).get_secret_value()}",
        }

    async def _fetch_observations_arrow(
        self,
        query: ObservationsQuery,
        query_engine: QueryEngine = "auto",
    ) -> pa.Table:
        """Internal async method to fetch observations as Arrow table."""
        async with self._httpx_client() as client:
            table = await _search_table(
                client=client,
                route=f"{self.warehouse_observations_endpoint}/search/stream/arrow",
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
            table = await _search_table(
                client=client,
                route=f"{self.warehouse_patrols_endpoint}/search/stream/arrow",
                query=query,
                headers=self._get_auth_headers(),
                store_type=query_engine,
            )
        return table

    async def _fetch_events_arrow(
        self,
        query: EventsQuery,
        query_engine: QueryEngine = "auto",
    ) -> pa.Table:
        """Internal async method to fetch events as Arrow table."""
        async with self._httpx_client() as client:
            table = await _search_table(
                client=client,
                route=f"{self.warehouse_events_endpoint}/search/stream/arrow",
                query=query,
                headers=self._get_auth_headers(),
                store_type=query_engine,
            )
        return table

    async def _fetch_event_types_arrow(
        self,
        query: EventTypesQuery,
        query_engine: QueryEngine = "auto",
    ) -> pa.Table:
        """Internal async method to fetch event types as Arrow table."""
        async with self._httpx_client() as client:
            table = await _get_table(
                client=client,
                route=self.warehouse_event_types_endpoint,
                query=query,
                headers=self._get_auth_headers(),
                store_type=query_engine,
            )
        return table

    async def _fetch_event_schema(
        self,
        query: EventTypeSchemaQuery,
        *,
        parse_detail_datetimes: bool,
        fmt: Literal["arrow", "json"],
        query_engine: QueryEngine = "auto",
    ) -> "pa.Schema | dict[str, Any]":
        """Fetch the event_details schema from the /events/schema endpoint.

        Unlike the streaming endpoints, /events/schema returns a bare Arrow
        *schema message* (read with ``pa.ipc.read_schema``), not an IPC stream,
        or — with ``fmt="json"`` — an informational ``{field: type_str}`` mapping.
        """
        params = query.model_dump(exclude_none=True)
        params["store_type"] = query_engine
        params["format"] = fmt
        if parse_detail_datetimes:
            params["parse_detail_datetimes"] = True
        async with self._httpx_client() as client:
            response = await client.get(
                f"{self.warehouse_events_endpoint}/schema",
                params=params,
                headers=self._get_auth_headers(),
                timeout=600,
            )
            response.raise_for_status()
            if fmt == "json":
                return response.json()
            return pa.ipc.read_schema(pa.py_buffer(response.content))

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
        filter: int | None = None,
        include_subject_additional: bool = False,
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
            include_subject_additional: Populate the subject ``additional`` JSON in
                ``extra__subject__additional``. Defaults to False, in which case the
                column is present but null and the JSON is not read. Opt in only when
                a consumer needs it (e.g. the ``rgb`` key for per-subject colouring).

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
            exclusion_flags=filter,
            include_subject_additional=include_subject_additional,
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
        patrols_overlap_daterange: bool = True,
        query_engine: QueryEngine | None = None,
        filter: int | None = None,
    ) -> pa.Table:
        """Get patrol observations filtered by patrol type and status.

        Args:
            since: Start of time range (ISO 8601 format).
            until: End of time range (ISO 8601 format).
            patrol_type_value: List of patrol type values to filter by.
            status: List of patrol statuses to filter by (e.g., ["done"]).
            include_patrol_details: Whether to include patrol metadata.
            sub_page_size: Ignored (for interface compatibility).
            patrols_overlap_daterange: If True (default), include patrols
                whose time range overlaps [since, until]; if False, include
                only patrols starting within that range.
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
            patrols_overlap_daterange=patrols_overlap_daterange,
            include_patrol_details=include_patrol_details,
            exclusion_flags=filter,
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
        patrols_overlap_daterange: bool = True,
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
            patrols_overlap_daterange: If True (default), include patrols
                whose time range overlaps [since, until]; if False, include
                only patrols starting within that range.
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
            patrols_overlap_daterange=patrols_overlap_daterange,
            include_events=False,
        )
        return self._run_async(self._fetch_patrols_arrow(query, query_engine=engine))

    def get_patrols(
        self,
        since: str | None = None,
        until: str | None = None,
        patrol_type_value: list[str] | None = None,
        status: list[str] | None = None,
        sub_page_size: int | None = None,
        patrols_overlap_daterange: bool = True,
        query_engine: QueryEngine | None = None,
    ) -> pa.Table:
        """Get patrols with their events from the EarthRanger Data Warehouse.

        Returns a ``pa.Table`` of patrols with events nested under each patrol
        segment at ``patrol_segments[].events[]`` (event geometry is WKB), in the
        ER-native shape.

        Args:
            since: Start of time range (ISO 8601 format). Optional.
            until: End of time range (ISO 8601 format). Optional.
            patrol_type_value: List of patrol type values to filter by.
            status: List of patrol statuses to filter by (e.g., ["done"]).
            sub_page_size: Ignored (for interface compatibility).
            patrols_overlap_daterange: If True (default), include patrols
                whose time range overlaps [since, until]; if False, include
                only patrols starting within that range.
            query_engine: Backend engine to use. Defaults to the client-level
                setting (``self.query_engine``).

        Returns:
            PyArrow Table of patrols, one row per patrol, with a
            ``patrol_segments`` list column whose segments each carry a nested
            ``events`` list (WKB geometry).
            Schema: PATROLS_WITH_EVENTS_NESTED_SCHEMA_V1.
        """
        engine = query_engine or self.query_engine
        query = PatrolsQuery(
            tenant_domain=self.server,
            range_start=datetime.fromisoformat(since) if since else None,
            range_end=datetime.fromisoformat(until) if until else None,
            patrol_type_value=patrol_type_value,
            patrol_status=status,  # type: ignore[arg-type]
            patrols_overlap_daterange=patrols_overlap_daterange,
            include_events=True,
        )
        return self._run_async(self._fetch_patrols_arrow(query, query_engine=engine))

    def get_patrol_events(
        self,
        since: str | None = None,
        until: str | None = None,
        patrol_type_value: list[str] | None = None,
        event_type: list[str] | None = None,
        status: list[str] | None = None,
        drop_null_geometry: bool = False,
        sub_page_size: int | None = None,
        query_engine: QueryEngine | None = None,
    ) -> pa.Table:
        """Get patrol events as a flat ``pa.Table``, one row per event.

        Fetches patrols with their nested events via ``get_patrols`` and
        flattens ``patrol_segments[].events[]`` to one row per event, attaching
        the patrol/segment context (``patrol_id``, ``patrol_serial_number``,
        ``patrol_segment_id``, ``patrol_type``, ``patrol_start_time``). Event
        geometry is the geoarrow WKB column (EPSG:4326).

        Args:
            since: Start of time range (ISO 8601 format). Optional.
            until: End of time range (ISO 8601 format). Optional.
            patrol_type_value: List of patrol type values to filter patrols by.
            event_type: If given, keep only events whose ``event_type`` is in the
                list.
            status: List of patrol statuses to filter by (e.g., ["done"]).
            drop_null_geometry: If True, exclude events with no geometry.
            sub_page_size: Ignored (for interface compatibility).
            query_engine: Backend engine to use. Defaults to the client-level
                setting (``self.query_engine``).

        Returns:
            PyArrow Table, one row per patrol event.
            Schema: PATROL_EVENTS_FLAT_SCHEMA_V1.
        """
        patrols = self.get_patrols(
            since=since,
            until=until,
            patrol_type_value=patrol_type_value,
            status=status,
            sub_page_size=sub_page_size,
            query_engine=query_engine,
        )
        wanted_types = set(event_type or [])
        rows: list[dict] = []
        for patrol in patrols.to_pylist():
            for segment in patrol.get("patrol_segments") or []:
                for event in segment.get("events") or []:
                    if wanted_types and event.get("event_type") not in wanted_types:
                        continue
                    if drop_null_geometry and event.get("geometry") is None:
                        continue
                    rows.append(
                        {
                            **event,
                            "patrol_id": patrol.get("id"),
                            "patrol_serial_number": patrol.get("serial_number"),
                            "patrol_segment_id": segment.get("id"),
                            "patrol_type": segment.get("patrol_type"),
                            "patrol_start_time": segment.get("time_range_start"),
                        }
                    )
        return pa.Table.from_pylist(rows, schema=PATROL_EVENTS_FLAT_SCHEMA_V1)

    def get_patrol_observations(
        self,
        patrols_df: Any,
        include_patrol_details: bool = True,
        sub_page_size: int | None = None,
        query_engine: QueryEngine | None = None,
        filter: int | None = None,
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
            exclusion_flags=filter,
        )
        return self._run_async(
            self._fetch_observations_arrow(query, query_engine=engine)
        )

    # -------------------------------------------------------------------------
    # EarthRangerClientProtocol implementation - Events
    # -------------------------------------------------------------------------

    def get_events(
        self,
        since: str | None = None,
        until: str | None = None,
        event_type: list[str] | None = None,
        drop_null_geometry: bool = False,
        include_details: bool = False,
        include_updates: bool = False,
        include_related_events: bool = False,
        *,
        raw_details: bool = False,
        parse_detail_datetimes: bool = False,
        invalid_details: Literal["drop", "coerce"] | None = None,
        invalid_only: bool = False,
        query_engine: QueryEngine | None = None,
    ) -> pa.Table:
        """Get events from the EarthRanger Data Warehouse.

        Args:
            since: Start of time range (ISO 8601 format). Optional.
            until: End of time range (ISO 8601 format). Optional.
            event_type: List of event type values to filter by.
            drop_null_geometry: If True, exclude events without geometry. Maps to
                the API's ``include_null_geometry`` (inverse).
            include_details: Include the ``event_details`` payload. When False
                (default) it is omitted entirely. On its own (``raw_details``
                False) it requests the typed struct derived from the event type's
                schema, which requires exactly one ``event_type`` (raises
                otherwise); it does NOT silently fall back to raw.
            include_updates: Unsupported; raises NotImplementedError.
            include_related_events: Unsupported; raises NotImplementedError.
            raw_details: Format override — return ``event_details`` as a flat JSON
                string (``EVENTS_SCHEMA_V1``) instead of the typed struct, which
                works across any number of event types. Composes with
                ``include_details`` (``include_details=True, raw_details=True`` is
                a valid "include details, but raw" request) and may also be used
                on its own. Mutually exclusive with the typed-detail options
                below.
            parse_detail_datetimes: In typed mode, best-effort parse of datetime
                strings inside ``event_details``. Requires exactly one event_type.
            invalid_details: In typed mode, how to handle ``event_details`` that
                fail schema validation: ``"drop"`` to drop the offending values,
                ``"coerce"`` to coerce them.
            invalid_only: In typed mode, return only rows whose ``event_details``
                fail schema validation. Requires exactly one event_type.
            query_engine: Backend engine to use. Defaults to the client-level
                setting (``self.query_engine``).

        Returns:
            PyArrow Table with events data. Schema: EVENTS_SCHEMA_V1
            (``event_details`` is a typed struct in typed mode, a JSON string
            with ``raw_details``, and null when details are omitted).

        Raises:
            NotImplementedError: If ``include_updates`` or
                ``include_related_events`` is requested; the warehouse does not
                serve event updates or related events.
            ValueError: If typed mode (``include_details`` /
                ``parse_detail_datetimes`` / ``invalid_only`` /
                ``invalid_details``, with ``raw_details`` False) is requested
                without exactly one event_type; or if ``raw_details`` is combined
                with ``parse_detail_datetimes`` / ``invalid_only`` /
                ``invalid_details``.
        """
        if include_updates or include_related_events:
            raise NotImplementedError(
                "include_updates and include_related_events are not supported by "
                "the Data Warehouse API; event updates and related events are not "
                "served."
            )

        event_type = list(event_type or [])
        n = len(event_type)

        # parse_detail_datetimes/invalid_only/invalid_details configure the typed
        # event_details struct, so they only make sense in typed mode.
        typed_only = invalid_details or parse_detail_datetimes or invalid_only

        if raw_details and typed_only:
            raise ValueError(
                "raw_details cannot be combined with parse_detail_datetimes, "
                "invalid_only, or invalid_details: those configure the typed "
                "event_details struct, which raw_details opts out of."
            )

        # raw_details is a format override: include_details + raw_details is a
        # valid "include details, but as raw JSON" request. The typed struct is
        # derived from a single event type's schema (heterogeneous types can't
        # share one Arrow struct), so typed mode requires exactly one event_type.
        want_typed = (include_details or typed_only) and not raw_details
        if want_typed and n != 1:
            raise ValueError(
                "include_details (and parse_detail_datetimes / invalid_only / "
                "invalid_details) require exactly one event_type, since the "
                "typed event_details struct is derived from a single event "
                "type's schema. Pass raw_details=True for raw JSON "
                "event_details across multiple event types."
            )

        # since/until are optional and may be half-bounded, matching the API,
        # EventsQuery, and EarthRangerIO.get_events; an omitted bound is dropped
        # from the query params. The detail-shaping options are fields on the
        # shared EventsQuery (single source of truth): event_details is included
        # when typed OR raw; raw_details picks the flat-JSON format.
        query = EventsQuery(
            tenant_domain=self.server,
            range_start=datetime.fromisoformat(since) if since else None,
            range_end=datetime.fromisoformat(until) if until else None,
            event_type=event_type or None,
            include_null_geometry=not drop_null_geometry,
            include_details=want_typed or raw_details,
            raw_details=raw_details,
            parse_detail_datetimes=parse_detail_datetimes,
            invalid_only=invalid_only,
            invalid_details=invalid_details or "drop",
        )

        engine = query_engine or self.query_engine
        return self._run_async(self._fetch_events_arrow(query, query_engine=engine))

    def get_event_types(self, query_engine: QueryEngine | None = None) -> pa.Table:
        """Get event types from the EarthRanger Data Warehouse.

        The returned table provides the ``value`` -> ``display`` (and
        ``category_value`` -> ``category_display``) mapping used to resolve event
        type and category display names.

        Args:
            query_engine: Backend engine to use. Defaults to the client-level
                setting (``self.query_engine``).

        Returns:
            PyArrow Table with event types. Schema: EVENT_TYPES_SCHEMA_V1
            (id, value, display, category_value, category_display, is_active,
            is_collection).
        """
        engine = query_engine or self.query_engine
        query = EventTypesQuery(tenant_domain=self.server)
        return self._run_async(
            self._fetch_event_types_arrow(query, query_engine=engine)
        )

    @overload
    def get_event_schema(
        self,
        event_type: str,
        *,
        parse_detail_datetimes: bool = ...,
        format: Literal["arrow"] = ...,
        query_engine: QueryEngine | None = ...,
    ) -> pa.Schema: ...

    @overload
    def get_event_schema(
        self,
        event_type: str,
        *,
        parse_detail_datetimes: bool = ...,
        format: Literal["json"],
        query_engine: QueryEngine | None = ...,
    ) -> dict[str, Any]: ...

    def get_event_schema(
        self,
        event_type: str,
        *,
        parse_detail_datetimes: bool = False,
        format: Literal["arrow", "json"] = "arrow",
        query_engine: QueryEngine | None = None,
    ) -> "pa.Schema | dict[str, Any]":
        """Discover the typed ``event_details`` schema for one event type.

        Reads the warehouse ``/events/schema`` discovery endpoint, which serves
        exactly one event type. This is a convenience for introspecting the
        ``event_details`` struct shape ahead of streaming; the same struct is
        embedded in ``/events/stream/arrow`` responses in typed mode, so this
        call is not required to consume events.

        Args:
            event_type: The single event type value (slug) or UUID to discover.
            parse_detail_datetimes: If True, return the datetime-typed variant
                (JSON-Schema ``date-time`` -> ``timestamp(ns, UTC)``, ``date`` ->
                ``date32``), matching what ``/events/stream/arrow`` emits for the
                same flag.
            format: ``"arrow"`` (default) returns a ``pa.Schema`` whose
                ``event_details`` field is the derived struct; ``"json"`` returns
                an informational ``{field: type_str}`` mapping.
            query_engine: Backend engine to use. Defaults to the client-level
                setting (``self.query_engine``).

        Returns:
            A ``pa.Schema`` (format="arrow") or a ``dict[str, Any]``
            (format="json").
        """
        engine = query_engine or self.query_engine
        query = EventTypeSchemaQuery(tenant_domain=self.server, event_type=event_type)
        return self._run_async(
            self._fetch_event_schema(
                query,
                parse_detail_datetimes=parse_detail_datetimes,
                fmt=format,
                query_engine=engine,
            )
        )

    def get_event_type_display_names_from_events(
        self,
        events_gdf: Any,
        append_category_names: str = "duplicates",
    ) -> Any:
        """Not implemented; the DWH client returns only pyarrow types.

        The ``value`` -> ``display`` (and ``category_value``) mapping needed to
        resolve event-type display names is available from ``get_event_types()``
        (a ``pa.Table``).
        """
        raise NotImplementedError(
            "get_event_type_display_names_from_events is not implemented in "
            "ERWarehouseClient, which returns only pyarrow types. The "
            "value->display mapping needed to resolve event-type display names "
            "is available from get_event_types() (a pa.Table)."
        )
