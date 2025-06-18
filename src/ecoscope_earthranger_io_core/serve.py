"""Utilities for use in server appliations."""

import asyncio
from io import BytesIO
from typing import AsyncIterable

import pyarrow as pa

from ecoscope_earthranger_io_core.arrow import SchemaConversion


async def generate_bytes(
    source_schema: pa.Schema,
    async_batch_generator: AsyncIterable[pa.RecordBatch],
    conversion: SchemaConversion | None = None,
) -> AsyncIterable[bytes]:
    sink = BytesIO()
    writer = pa.ipc.new_stream(sink, source_schema)
    if conversion:
        # ensure schema compatibility before we enter the serving loop
        assert conversion.source_schema.equals(source_schema), (
            f"Provided conversion has `.source_schema`:\n{conversion.source_schema}\n"
            f"which does not match the `source_schema` argument:\n{source_schema}"
        )
    try:
        async for batch in async_batch_generator:
            if conversion:
                batch = conversion.convert(batch)
            sink.seek(0)
            sink.truncate(0)
            await asyncio.to_thread(writer.write_batch, batch)
            sink.seek(0)
            yield sink.getvalue()
    finally:
        await asyncio.to_thread(writer.close)
