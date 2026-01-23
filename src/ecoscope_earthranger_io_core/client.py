import asyncio
import io
from contextlib import asynccontextmanager
from datetime import datetime
from functools import cached_property
from typing import Any

import httpx
import pyarrow as pa
from pydantic import BaseModel, SecretStr

from ecoscope_earthranger_io_core.query import ObservationsQuery, PatrolsQuery


async def _get_table(
    client: httpx.AsyncClient,
    route: str,
    query: BaseModel,
    headers: dict[str, str] | None = None,
) -> pa.Table:
    """Fetch Arrow IPC stream from the warehouse API and return as a PyArrow Table.

    Args:
        client: The httpx async client.
        route: The API route to call.
        query: A Pydantic model specifying query parameters.
        headers: Optional headers to include.
    """
    async with client.stream(
        "GET",
        route,
        params=query.model_dump(exclude_none=True),
        headers=headers,
        timeout=600,
    ) as response:
        response.raise_for_status()
        sink = io.BytesIO()
        async for chunk in response.aiter_bytes():
            sink.write(chunk)
        sink.seek(0)
    source = sink.getvalue()
    table = pa.ipc.open_stream(source).read_all()
    return table


class ERWarehouseClient(BaseModel):
    """EarthRanger Warehouse Client.

    A client for fetching observations data from the EarthRanger Data Warehouse API.
    Implements the EarthRangerClientProtocol interface for use as a drop-in replacement
    for EarthRangerIO / EarthRangerClient in ecoscope-workflows.

    Example:
        >>> from pydantic import SecretStr
        >>> client = ERWarehouseClient(
        ...     server="mep-dev.pamdas.org",
        ...     token=SecretStr("your-api-token"),
        ...     warehouse_base_url="https://warehouse.pamdas.org",
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
    warehouse_base_url: str
    warehouse_observations_endpoint: str = "/observations"
    warehouse_patrols_endpoint: str = "/patrols"

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

    @asynccontextmanager
    async def _httpx_client(self):
        async with httpx.AsyncClient(base_url=self.warehouse_base_url) as client:
            yield client

    def _get_auth_headers(self) -> dict[str, str]:
        """Return authentication headers for API requests."""
        return {"X-EarthRanger-API-Token": self._token.get_secret_value()}

    async def _fetch_observations_arrow(
        self,
        query: ObservationsQuery,
    ) -> pa.Table:
        """Internal async method to fetch observations as Arrow table."""
        async with self._httpx_client() as client:
            table = await _get_table(
                client=client,
                route=f"{self.warehouse_observations_endpoint}/stream/arrow",
                query=query,
                headers=self._get_auth_headers(),
            )
        return table

    async def _fetch_patrols_arrow(
        self,
        query: PatrolsQuery,
    ) -> pa.Table:
        """Internal async method to fetch patrols as Arrow table."""
        async with self._httpx_client() as client:
            table = await _get_table(
                client=client,
                route=f"{self.warehouse_patrols_endpoint}/stream/arrow",
                query=query,
                headers=self._get_auth_headers(),
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

        Returns:
            PyArrow Table with observations data.
        """
        if since is None or until is None:
            raise ValueError("Both 'since' and 'until' must be provided")

        query = ObservationsQuery(
            tenant_domain=self.server,
            range_start=datetime.fromisoformat(since),
            range_end=datetime.fromisoformat(until),
            subject_group_name=subject_group_name,
        )
        table = self._run_async(self._fetch_observations_arrow(query))
        return table

    def get_patrol_observations_with_patrol_filter(
        self,
        since: str | None = None,
        until: str | None = None,
        patrol_type_value: list[str] | None = None,
        status: list[str] | None = None,
        include_patrol_details: bool = True,
        sub_page_size: int | None = None,
    ) -> pa.Table:
        """Get patrol observations filtered by patrol type and status.

        Args:
            since: Start of time range (ISO 8601 format).
            until: End of time range (ISO 8601 format).
            patrol_type_value: List of patrol type values to filter by.
            status: List of patrol statuses to filter by (e.g., ["done"]).
            include_patrol_details: Whether to include patrol metadata.
            sub_page_size: Ignored (for interface compatibility).

        Returns:
            PyArrow Table with patrol observations data including patrol metadata.
        """
        if since is None or until is None:
            raise ValueError("Both 'since' and 'until' must be provided")

        query = ObservationsQuery(
            tenant_domain=self.server,
            range_start=datetime.fromisoformat(since),
            range_end=datetime.fromisoformat(until),
            patrol_type_value=patrol_type_value,
            patrol_status=status,  # type: ignore[arg-type]
            include_patrol_details=include_patrol_details,
        )
        table = self._run_async(self._fetch_observations_arrow(query))
        return table

    def get_patrols(
        self,
        since: str,
        until: str,
        patrol_type_value: list[str] | None = None,
        status: list[str] | None = None,
        sub_page_size: int | None = None,
    ) -> pa.Table:
        """Get patrols from EarthRanger Data Warehouse.

        Args:
            since: Start of time range (ISO 8601 format).
            until: End of time range (ISO 8601 format).
            patrol_type_value: List of patrol type values to filter by.
            status: List of patrol statuses to filter by (e.g., ["done"]).
            sub_page_size: Ignored (for interface compatibility).

        Returns:
            PyArrow Table with patrols data (nested format with patrol_segments).
        """
        query = PatrolsQuery(
            tenant_domain=self.server,
            range_start=datetime.fromisoformat(since),
            range_end=datetime.fromisoformat(until),
            patrol_type_value=patrol_type_value,
            patrol_status=status,
        )
        return self._run_async(self._fetch_patrols_arrow(query))

    def get_patrol_observations(
        self,
        patrols_df: Any,
        include_patrol_details: bool = True,
        sub_page_size: int | None = None,
    ) -> pa.Table:
        """Get observations for patrols from EarthRanger Data Warehouse.

        Args:
            patrols_df: PyArrow Table or Pandas DataFrame with patrol data
                (as returned by get_patrols).
            include_patrol_details: Whether to include patrol metadata.
            sub_page_size: Ignored (for interface compatibility).

        Returns:
            PyArrow Table with patrol observations data.
        """
        # Handle both PyArrow Table and Pandas DataFrame
        if hasattr(patrols_df, "column"):  # PyArrow Table
            patrol_ids = patrols_df.column("id").to_pylist()
            segments_col = patrols_df.column("patrol_segments").to_pylist()
        else:  # Pandas DataFrame
            patrol_ids = patrols_df["id"].tolist()
            segments_col = patrols_df["patrol_segments"].tolist()

        # Flatten all segments from all patrols
        all_segments = [
            seg for segments in segments_col if segments for seg in segments
        ]

        # Extract time ranges (warehouse API uses time_range_start/end)
        time_starts = [
            s["time_range_start"] for s in all_segments if s.get("time_range_start")
        ]
        time_ends = [
            s["time_range_end"] for s in all_segments if s.get("time_range_end")
        ]

        if not time_starts or not time_ends:
            raise ValueError("Cannot determine time range from patrol data")

        # Normalize Z suffix to +00:00 for fromisoformat compatibility
        range_start = min(time_starts).replace("Z", "+00:00")
        range_end = max(time_ends).replace("Z", "+00:00")

        query = ObservationsQuery(
            tenant_domain=self.server,
            range_start=datetime.fromisoformat(range_start),
            range_end=datetime.fromisoformat(range_end),
            patrol_ids=list(set(patrol_ids)),
            include_patrol_details=include_patrol_details,
        )
        return self._run_async(self._fetch_observations_arrow(query))

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
