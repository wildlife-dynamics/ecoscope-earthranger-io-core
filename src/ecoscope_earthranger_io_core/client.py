import io
import warnings
from contextlib import asynccontextmanager
from datetime import datetime
from functools import cached_property

import httpx
import pyarrow as pa
from pydantic import BaseModel, SecretStr

from ecoscope_earthranger_io_core.query import ObservationsQuery


async def _get_table(
    client: httpx.AsyncClient,
    route: str,
    query: ObservationsQuery,
    headers: dict[str, str] | None = None,
):
    async with client.stream(
        "GET",
        route,
        params=query.model_dump(),
        headers=headers,
        timeout=60,
    ) as response:
        sink = io.BytesIO()
        async for chunk in response.aiter_bytes():
            sink.write(chunk)
        sink.seek(0)
    source = sink.getvalue()
    table = pa.ipc.open_stream(source).read_all()
    return table


class ERWarehouseClient(BaseModel):
    # user-facing
    server: str
    username: str
    password: SecretStr | None = None
    token: SecretStr | None = None

    # platform-level
    warehouse_base_url: str
    warehouse_events_router: str = "/events"
    warehouse_observations_router: str = "/observations"
    warehouse_patrol_events_router: str = "/patrol/events"
    warehouse_patrol_observations_router: str = "/patrol/observations"

    def _login(self) -> None:
        raise NotImplementedError(
            "Login not yet implemented, please pass `token` to constructor."
        )

    @cached_property
    def _token(self) -> SecretStr:
        if not self.token:
            self._login()
        return self.token

    def _subject_group_name_to_subject_ids(self, subject_group_name: str) -> list[str]:
        # TODO: use self.server + self.username to compute visibility
        return ["subject1", "subject2"]  # FIXME

    @asynccontextmanager
    async def _httpx_client(self):
        async with httpx.AsyncClient(base_url=self.warehouse_base_url) as client:
            yield client

    async def get_subjectgroup_observations(
        self,
        subject_group_name: str,
        since: str,
        until: str,
        include_subject_details: bool = True,
        include_inactive: bool = True,
        include_details: bool = True,
    ) -> pa.Table:
        """ """
        subject_ids = self._subject_group_name_to_subject_ids(subject_group_name)
        table = await self.get_subject_observations(
            subject_ids=subject_ids,
            since=since,
            until=until,
            include_subject_details=include_subject_details,
            include_inactive=include_inactive,
            include_details=include_details,
        )
        return table

    async def get_subject_observations(
        self,
        subject_ids: list[str],
        since: str,
        until: str,
        include_subject_details: bool = True,
        include_inactive: bool = True,
        include_details: bool = True,
    ) -> pa.Table:
        warnings.warn(
            f"Arguments {include_subject_details= }, {include_inactive= }, {include_details= } "
            "are supported for interface compatibility with ecoscope.io.earthranger.EarthRangerIO, but "
            f"the values passed to this arguments are currently ignored by {self.__class__.__name__}."
        )
        query = ObservationsQuery(
            tenant_domain=self.server,
            range_start=datetime.fromisoformat(since),
            range_end=datetime.fromisoformat(until),
            subject_ids=subject_ids,
        )
        async with self._httpx_client() as client:
            table = await _get_table(
                client=client,
                route=f"{self.warehouse_observations_router}/stream/arrow",
                query=query,
                headers={"X-EarthRanger-API-Token": self._token.get_secret_value()},
            )
        return table
