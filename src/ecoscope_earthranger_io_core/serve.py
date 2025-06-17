"""Utilities for use in server appliations."""

import asyncio
from io import BytesIO
from typing import AsyncIterable

import pyarrow as pa

from ecoscope_earthranger_io_core.arrow import SchemaConversion


async def generate_bytes(
    earthranger_schema: pa.Schema,
    async_batch_generator: AsyncIterable[pa.RecordBatch],
    conversion: SchemaConversion | None = None,
) -> AsyncIterable[bytes]:
    sink = BytesIO()
    writer = pa.ipc.new_stream(sink, earthranger_schema)
    if conversion:
        # ensure schema compatibility before we enter the serving loop
        assert conversion.earthranger_schema.equals(earthranger_schema), (
            f"Provided conversion has `.earthranger_schema`:\n{conversion.earthranger_schema}\n"
            f"which does not match the earthranger schema argument:\n{earthranger_schema}"
        )
    try:
        async for batch in async_batch_generator:
            if conversion:
                batch = conversion.to_ecoscope_rb(batch)
            sink.seek(0)
            sink.truncate(0)
            await asyncio.to_thread(writer.write_batch, batch)
            sink.seek(0)
            yield sink.getvalue()
    finally:
        await asyncio.to_thread(writer.close)
