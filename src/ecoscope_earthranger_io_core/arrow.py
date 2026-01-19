import asyncio
from dataclasses import dataclass
from enum import Enum
from functools import cached_property
from io import BytesIO
from typing import AsyncIterable, Callable, cast

import geoarrow.pyarrow  # type: ignore[import-untyped]
import pyarrow as pa


OBSERVATIONS_SCHEMA__EARTHRANGER_FULL_V1 = pa.schema(
    [
        ("created_at", pa.string()),
        ("exclusion_flags", pa.string()),
        ("is_active", pa.string()),
        ("location", geoarrow.pyarrow.wkb()),
        ("manufacturer_id", pa.string()),
        ("recorded_at", pa.string()),
        ("subject_id", pa.string()),
        ("subject_name", pa.string()),
        ("subject_subtype_id", pa.string()),
        ("das_tenant_id", pa.string()),
        ("domain", pa.string()),
        ("observation_id", pa.string()),
        ("source_id", pa.string()),
    ],
)
OBSERVATIONS_SCHEMA__ECOSCOPE_SLIM_V1 = pa.schema(
    [
        ("geometry", geoarrow.pyarrow.wkb()),
        ("fixtime", pa.timestamp("ns", tz="UTC")),
        ("groupby_col", pa.string()),
        ("extra__subject__name", pa.string()),
        ("extra__subject__subject_subtype", pa.string()),
        ("junk_status", pa.bool_()),
    ]
)

OBSERVATIONS_WITH_PATROL_SCHEMA_SLIM_V1 = pa.schema(
    [
        ("geometry", geoarrow.pyarrow.wkb()),
        ("fixtime", pa.timestamp("ns", tz="UTC")),
        ("groupby_col", pa.string()),
        ("extra__subject__name", pa.string()),
        ("extra__subject__subject_subtype", pa.string()),
        ("junk_status", pa.bool_()),
        ("patrol_id", pa.string()),
        ("patrol_title", pa.string()),
        ("patrol_serial_number", pa.int64()),
        ("patrol_status", pa.string()),
        ("patrol_type__value", pa.string()),  # Double underscore to match EarthRangerIO
        (
            "patrol_type__display",
            pa.string(),
        ),  # Double underscore to match EarthRangerIO
        ("patrol_start_time", pa.string()),
        ("patrol_end_time", pa.string()),
    ]
)


def _observations_pre_cast(earthranger_rb: pa.RecordBatch) -> pa.RecordBatch:
    """Convert an EarthRanger RecordBatch to an Ecoscope RecordBatch."""

    junk_status = pa.array([False] * earthranger_rb.num_rows, type=pa.bool_())
    add_junk_status = earthranger_rb.append_column("junk_status", junk_status)
    renamed = add_junk_status.rename_columns(
        {
            "location": "geometry",
            "subject_id": "groupby_col",
            "recorded_at": "fixtime",
            "subject_name": "extra__subject__name",
            "subject_subtype_id": "extra__subject__subject_subtype",
        }
    )
    # NOTE: workaround for missing +00:00 timezone offset in EarthRanger data, can be removed
    # once EarthRanger data is fixed to include timezone offsets.
    fixtime_idx = renamed.schema.get_field_index("fixtime")
    fixtime_naive = cast(list[str], renamed.column("fixtime").to_pylist())
    fixtime_utc = [t + "+00:00" for t in fixtime_naive]
    return renamed.drop_columns("fixtime").add_column(
        fixtime_idx, "fixtime", fixtime_utc
    )


class SchemaChoices(str, Enum):
    EARTHRANGER_FULL_V1 = "EARTHRANGER_FULL_V1"
    ECOSCOPE_SLIM_V1 = "ECOSCOPE_SLIM_V1"


def _subset_schema(schema: pa.Schema, fields: list[str]) -> pa.Schema:
    """Return a new schema with only specified subset of fields retained."""
    return pa.schema([field for field in schema if field.name in fields])


@dataclass(frozen=True)
class TransformSpec:
    persisted_schema: pa.Schema  # the "on disk" representation
    target_schema: pa.Schema | None = (
        None  # a different schema to convert to, if desired
    )
    required_columns: list[str] | None = (
        None  # columns from the "on disk" repr that are required to realize this transformation
    )
    pre_cast_fn: Callable[[pa.RecordBatch], pa.RecordBatch] | None = None
    post_cast_fn: Callable[[pa.RecordBatch], pa.RecordBatch] | None = None

    @cached_property
    def pre_transform_schema(self) -> pa.Schema:
        """Return the schema to use before any transformation."""
        if self.required_columns:
            return _subset_schema(self.persisted_schema, self.required_columns)
        return self.persisted_schema

    def transform(self, input_rb: pa.RecordBatch) -> pa.RecordBatch:
        """Transform an input RecordBatch to a RecordBatch with the target schema."""
        _rb = input_rb.cast(self.pre_transform_schema)
        if self.pre_cast_fn:
            _rb = self.pre_cast_fn(_rb)
        if self.target_schema:
            _rb = _rb.cast(self.target_schema)
        if self.post_cast_fn:
            _rb = self.post_cast_fn(_rb)
        return _rb

    @property
    def stream_schema(self) -> pa.Schema:
        """The schema of the stream that will be returned by the transformation."""
        return self.target_schema or self.persisted_schema

    async def generate_bytes(
        self,
        async_batch_generator: AsyncIterable[pa.RecordBatch],
    ) -> AsyncIterable[bytes]:
        sink = BytesIO()
        writer = pa.ipc.new_stream(sink, self.stream_schema)
        try:
            async for batch in async_batch_generator:
                if self.target_schema:
                    batch = self.transform(batch)
                sink.seek(0)
                sink.truncate(0)
                await asyncio.to_thread(writer.write_batch, batch)
                sink.seek(0)
                yield sink.getvalue()
        finally:
            await asyncio.to_thread(writer.close)


TRANSFORMS: dict[SchemaChoices, TransformSpec] = {
    SchemaChoices.EARTHRANGER_FULL_V1: TransformSpec(
        persisted_schema=OBSERVATIONS_SCHEMA__EARTHRANGER_FULL_V1
    ),
    SchemaChoices.ECOSCOPE_SLIM_V1: TransformSpec(
        persisted_schema=OBSERVATIONS_SCHEMA__EARTHRANGER_FULL_V1,
        required_columns=[
            "location",
            "recorded_at",
            "subject_id",
            "subject_name",
            "subject_subtype_id",
        ],
        target_schema=OBSERVATIONS_SCHEMA__ECOSCOPE_SLIM_V1,
        pre_cast_fn=_observations_pre_cast,
    ),
}
