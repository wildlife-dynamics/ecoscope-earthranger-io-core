import io

import httpx
import pyarrow as pa

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
