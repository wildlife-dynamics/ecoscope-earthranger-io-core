import io

import httpx
import pyarrow as pa

from ecoscope_earthranger_io_core.query import ObservationsQuery


async def get_table(
    route: str,
    query: ObservationsQuery | None,
    base_url: str,
    headers: dict[str, str] | None = None,
):
    async with httpx.AsyncClient(base_url=base_url) as ac:
        async with ac.stream(
            "GET",
            route,
            params=query.model_dump() if query else None,
            headers=headers,
            timeout=60,
        ) as response:
            sink = io.BytesIO()
            async for chunk in response.aiter_bytes():
                await sink.write(chunk)
            sink.seek(0)
        source = sink.getvalue()
        table = pa.ipc.open_stream(source).read_all()
        return table
