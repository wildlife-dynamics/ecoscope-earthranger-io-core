import io
from datetime import datetime
from functools import cached_property

import httpx
import pyarrow as pa
from pydantic import BaseModel, SecretStr

from ecoscope_earthranger_io_core.query import ObservationsQuery


async def get_table(
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
    _warehouse_base_url: str
    _warehouse_events_router: str | None = None
    _warehouse_observations_router: str
    _warehouse_patrol_events_router: str | None = None
    _warehouse_patrol_observations_router: str | None = None

    @cached_property
    def _tenant_id(self) -> str:
        # TODO: use self.server to compute
        return "123"  # FIXME

    def _subject_group_name_to_subject_ids(self, subject_group_name: str) -> list[str]:
        # TODO: use self.server + self.username to compute visibility
        return ["subject1", "subject2"]  # FIXME

    async def get_subjectgroup_observations(
        self,
        subject_group_name: str,
        since: str,
        until: str,
        include_subject_details: bool = True,
        include_inactive: bool = True,
        include_details: bool = True,
    ):
        """ """
        subject_ids = self._subject_group_name_to_subject_ids(subject_group_name)
        query = ObservationsQuery(
            tenant_id=self._tenant_id,
            range_start=datetime.fromisoformat(since),
            range_end=datetime.fromisoformat(until),
            subject_ids=subject_ids,
        )
        # TODO: how do we cache a per-instance client, while still using a context manager?
        async with httpx.AsyncClient(base_url=self._warehouse_base_url) as client:
            table = await get_table(
                client=client,
                route="/stream/arrow",
                query=query,
                headers=None,
            )

        return table
