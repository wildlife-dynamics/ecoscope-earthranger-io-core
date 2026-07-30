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
        # Raw `subjects.additional` JSON. EarthRanger declares no schema for it (unlike
        # event types, which carry an EventType.schema), so it is carried verbatim as a
        # string and parsed by consumers. Holds the per-subject `rgb` used for track
        # colouring, plus `sex`/`region`/`country`/`species`.
        #
        # NULL means "not requested", and stores must keep it that way:
        #     include_subject_additional -> COALESCE(s.additional, '{}')
        #     otherwise                  -> CAST(NULL AS STRING)
        # The COALESCE is required because the subjects join is a LEFT JOIN (the same
        # reason the sibling columns use COALESCE(s.name, 'unknown')); a bare
        # `s.additional` would also yield NULL for observations whose source has no
        # subjectsource assignment, collapsing "not requested" and "subject
        # unresolved" into one value. `{}` is safe as the resolved-but-empty marker:
        # the CDC never writes NULL additional (it coerces to '{}').
        ("subject_additional", pa.string()),
        ("das_tenant_id", pa.string()),
        ("domain", pa.string()),
        ("observation_id", pa.string()),
        ("source_id", pa.string()),
    ],
)
OBSERVATIONS_SCHEMA__ECOSCOPE_SLIM_V1 = pa.schema(
    [
        ("geometry", geoarrow.pyarrow.wkb().with_crs("EPSG:4326")),
        ("fixtime", pa.timestamp("ns", tz="UTC")),
        ("groupby_col", pa.string()),
        ("extra__subject__name", pa.string()),
        ("extra__subject__subject_subtype", pa.string()),
        # Raw subject `additional` JSON as a string; ecoscope strips the `extra__`
        # prefix to `subject__additional`, matching the EarthRanger API path where the
        # nested dict arrives under the same name. Parsed by consumers (e.g.
        # `assign_subject_colors` reads the `rgb` key) -- EarthRanger declares no
        # schema for this field, so it is not decomposed into typed columns here.
        # Always present so the stream schema is stable, but null unless the query
        # sets `include_subject_additional` (same opt-in shape as `event_details`).
        ("extra__subject__additional", pa.string()),
        ("extra__source", pa.string()),
        ("junk_status", pa.bool_()),
    ]
)

OBSERVATIONS_WITH_PATROL_SCHEMA_SLIM_V1 = pa.schema(
    [
        ("geometry", geoarrow.pyarrow.wkb().with_crs("EPSG:4326")),
        ("fixtime", pa.timestamp("ns", tz="UTC")),
        # groupby_col carries the patrol id (one trajectory per patrol), matching
        # EarthRangerIO.get_patrol_observations. The leader subject is exposed via
        # patrol_subject (name) and extra__subject_id (id) rather than groupby_col.
        ("groupby_col", pa.string()),
        ("extra__subject_id", pa.string()),
        ("patrol_subject", pa.string()),
        ("extra__source", pa.string()),
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

# =========================================================================
# Patrol Schemas
# =========================================================================

# Struct type for patrol segments (used in nested schema)
PATROL_SEGMENT_STRUCT_V1 = pa.struct(
    [
        ("id", pa.string()),
        ("patrol_type", pa.string()),
        ("patrol_type_display", pa.string()),
        ("leader_id", pa.string()),
        ("time_range_start", pa.string()),
        ("time_range_end", pa.string()),
        ("scheduled_start", pa.string()),
        ("scheduled_end", pa.string()),
        ("start_location", pa.string()),
        ("end_location", pa.string()),
    ]
)

# Nested schema: one row per patrol with patrol_segments as list of structs
PATROLS_NESTED_SCHEMA_V1 = pa.schema(
    [  # type: ignore[arg-type]
        ("id", pa.string()),
        ("serial_number", pa.int64()),
        ("priority", pa.int64()),
        ("state", pa.string()),
        ("title", pa.string()),
        ("objective", pa.string()),
        ("created_at", pa.string()),
        ("updated_at", pa.string()),
        ("patrol_segments", pa.list_(PATROL_SEGMENT_STRUCT_V1)),
    ]
)

# Flat schema: one row per patrol-segment combination
PATROLS_FLAT_SCHEMA_V1 = pa.schema(
    [  # type: ignore[arg-type]
        ("id", pa.string()),
        ("serial_number", pa.int64()),
        ("priority", pa.int64()),
        ("state", pa.string()),
        ("title", pa.string()),
        ("objective", pa.string()),
        ("created_at", pa.string()),
        ("updated_at", pa.string()),
        ("segment_id", pa.string()),
        ("patrol_type", pa.string()),
        ("patrol_type_display", pa.string()),
        ("leader_id", pa.string()),
        ("time_range_start", pa.string()),
        ("time_range_end", pa.string()),
        ("scheduled_start", pa.string()),
        ("scheduled_end", pa.string()),
        ("start_location", pa.string()),
        ("end_location", pa.string()),
    ]
)

# Patrol-only schema: no segment columns
PATROLS_ONLY_SCHEMA_V1 = pa.schema(
    [  # type: ignore[arg-type]
        ("id", pa.string()),
        ("serial_number", pa.int64()),
        ("priority", pa.int64()),
        ("state", pa.string()),
        ("title", pa.string()),
        ("objective", pa.string()),
        ("created_at", pa.string()),
        ("updated_at", pa.string()),
    ]
)


# =========================================================================
# Event Schemas
# =========================================================================

# reported_by: the event reporter, normalized server-side to a uniform shape
# whether it points at a subject or a user. ``type`` is the discriminator
# ("subject" | "user" | ... | null) and ``name`` is resolved per type (subject
# name / user display-name / null for source|community); ``id`` is the reporter
# id. A single struct covers every reporter kind because the polymorphism lives
# in the values, not the shape -- the subject/user branch is collapsed before
# serialization, so there is no per-type field divergence and no union needed.
REPORTED_BY_STRUCT_V1 = pa.struct(
    [
        ("id", pa.string()),
        ("name", pa.string()),
        ("type", pa.string()),
    ]
)

# Flat events schema (one row per event). The root-level timestamps
# (event_time/end_time/created_at/updated_at) are typed Arrow
# ``timestamp[ns, UTC]``, matching the observations ecoscope-facing default
# (``fixtime``) -- they're DB-typed end-to-end (PG timestamp -> Debezium ->
# Iceberg TimestampType), so typing is lossless and never invalid. (This is
# distinct from ``event_details`` datetimes, which live in free-form JSON and
# stay opt-in/best-effort.) ``event_details`` is a JSON string here; in typed
# mode the API swaps in a struct derived from the event type's JSON-Schema via a
# pre_cast (the flat schema is unaffected). ``reported_by`` is a typed
# ``{id, name, type}`` struct (see REPORTED_BY_STRUCT_V1). ``geometry`` is the
# EventGeometry polygon when present else the Point from the event location.
EVENTS_SCHEMA_V1 = pa.schema(
    [  # type: ignore[arg-type]
        ("id", pa.string()),
        ("serial_number", pa.int64()),
        ("event_type_id", pa.string()),
        ("event_type_value", pa.string()),
        ("event_category_value", pa.string()),
        ("title", pa.string()),
        ("state", pa.string()),
        ("priority", pa.int64()),
        ("event_time", pa.timestamp("ns", tz="UTC")),
        ("end_time", pa.timestamp("ns", tz="UTC")),
        ("created_at", pa.timestamp("ns", tz="UTC")),
        ("updated_at", pa.timestamp("ns", tz="UTC")),
        ("is_collection", pa.bool_()),
        ("geometry", geoarrow.pyarrow.wkb().with_crs("EPSG:4326")),
        ("reported_by", REPORTED_BY_STRUCT_V1),
        ("event_details", pa.string()),
        ("das_tenant_id", pa.string()),
    ]
)

# Struct type for events nested inside a patrol (patrols include_events).
# ``geometry`` is plain WKB ``binary`` (not the geoarrow extension used by the
# flat EVENTS_SCHEMA_V1): an extension type cannot be built inside a
# list<struct<...>> via pyarrow's from_pylist (the nesting path), and the bytes
# are the same WKB the consumer reads either way.
PATROL_EVENT_STRUCT_V1 = pa.struct(
    [
        ("id", pa.string()),
        ("serial_number", pa.int64()),
        ("event_type", pa.string()),
        ("event_time", pa.timestamp("ns", tz="UTC")),
        ("priority", pa.int64()),
        ("title", pa.string()),
        ("state", pa.string()),
        ("updated_at", pa.string()),
        ("created_at", pa.string()),
        ("geometry", pa.binary()),
        ("is_collection", pa.bool_()),
        ("event_details", pa.string()),
    ]
)

# Patrol-segment struct carrying its events. This is the segment struct used by
# the with-events schema only: it is PATROL_SEGMENT_STRUCT_V1's fields plus the
# resolved segment ``leader_name`` (from a subjects join on ``leader_id``) and a
# trailing ``events`` list. PATROL_SEGMENT_STRUCT_V1 itself is left untouched, so
# the lean/flat patrols schemas do not require the leader-name resolution.
PATROL_SEGMENT_WITH_EVENTS_STRUCT_V1 = pa.struct(
    list(PATROL_SEGMENT_STRUCT_V1)
    + [
        pa.field("leader_name", pa.string()),
        pa.field("events", pa.list_(PATROL_EVENT_STRUCT_V1)),
    ]
)

# Nested patrols schema WITH events — selected only when include_events=true.
# This is a NEW versioned schema: PATROLS_NESTED_SCHEMA_V1 is left untouched
# (never mutate a published versioned schema). It has the same top-level columns
# as PATROLS_NESTED_SCHEMA_V1, but each patrol segment carries its own events
# (events are nested under ``patrol_segments[].events[]``, not at the top level).
PATROLS_WITH_EVENTS_NESTED_SCHEMA_V1 = pa.schema(
    [  # type: ignore[arg-type]
        ("id", pa.string()),
        ("serial_number", pa.int64()),
        ("priority", pa.int64()),
        ("state", pa.string()),
        ("title", pa.string()),
        ("objective", pa.string()),
        ("created_at", pa.string()),
        ("updated_at", pa.string()),
        ("patrol_segments", pa.list_(PATROL_SEGMENT_WITH_EVENTS_STRUCT_V1)),
    ]
)

# Flat one-row-per-event schema produced by the client by flattening the nested
# PATROLS_WITH_EVENTS_NESTED_SCHEMA_V1 (events extracted from each segment, with
# their patrol/segment context attached). The event fields mirror
# PATROL_EVENT_STRUCT_V1, except ``geometry`` is the geoarrow WKB extension here
# (a top-level column can carry it, unlike a list<struct> field).
PATROL_EVENTS_FLAT_SCHEMA_V1 = pa.schema(
    [  # type: ignore[arg-type]
        ("id", pa.string()),
        ("serial_number", pa.int64()),
        ("event_type", pa.string()),
        ("event_time", pa.timestamp("ns", tz="UTC")),
        ("priority", pa.int64()),
        ("title", pa.string()),
        ("state", pa.string()),
        ("updated_at", pa.string()),
        ("created_at", pa.string()),
        ("geometry", geoarrow.pyarrow.wkb().with_crs("EPSG:4326")),
        ("is_collection", pa.bool_()),
        ("event_details", pa.string()),
        ("patrol_id", pa.string()),
        ("patrol_serial_number", pa.int64()),
        ("patrol_segment_id", pa.string()),
        ("patrol_type", pa.string()),
        ("patrol_start_time", pa.string()),
    ]
)

# Listing schema for the warehouse ``GET /event_types`` endpoint — the ecoscope
# ``get_event_types`` contract (display-name resolution).
EVENT_TYPES_SCHEMA_V1 = pa.schema(
    [  # type: ignore[arg-type]
        ("id", pa.string()),
        ("value", pa.string()),
        ("display", pa.string()),
        ("category_value", pa.string()),
        ("category_display", pa.string()),
        ("is_active", pa.bool_()),
        ("is_collection", pa.bool_()),
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
            "source_id": "extra__source",
            "subject_name": "extra__subject__name",
            "subject_subtype_id": "extra__subject__subject_subtype",
            "subject_additional": "extra__subject__additional",
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
    EVENTS_FLAT_V1 = "EVENTS_FLAT_V1"
    PATROLS_WITH_EVENTS_NESTED_V1 = "PATROLS_WITH_EVENTS_NESTED_V1"


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
            "source_id",
            "subject_name",
            "subject_subtype_id",
            "subject_additional",
        ],
        target_schema=OBSERVATIONS_SCHEMA__ECOSCOPE_SLIM_V1,
        pre_cast_fn=_observations_pre_cast,
    ),
    # Passthrough specs: the events flat schema and the patrols-with-events
    # nested schema stream as-is. The typed ``event_details`` struct (and its
    # datetime-typed variant) is swapped in dynamically by the API at request
    # time from the event type's JSON-Schema, not via a static target_schema.
    SchemaChoices.EVENTS_FLAT_V1: TransformSpec(
        persisted_schema=EVENTS_SCHEMA_V1,
    ),
    SchemaChoices.PATROLS_WITH_EVENTS_NESTED_V1: TransformSpec(
        persisted_schema=PATROLS_WITH_EVENTS_NESTED_SCHEMA_V1,
    ),
}
