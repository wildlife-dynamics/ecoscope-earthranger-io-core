from datetime import datetime, timezone
from typing import Literal

import geoarrow.pyarrow as ga  # type: ignore[import-untyped]
import pyarrow as pa
from fastapi import APIRouter, Depends, FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse, Response, StreamingResponse

import shapely.geometry
import shapely.wkb

from ecoscope_earthranger_io_core.arrow import (
    EVENT_TYPES_SCHEMA_V1,
    EVENTS_SCHEMA_V1,
    OBSERVATIONS_WITH_PATROL_SCHEMA_SLIM_V1,
    PATROLS_NESTED_SCHEMA_V1,
    PATROLS_WITH_EVENTS_NESTED_SCHEMA_V1,
    TRANSFORMS,
    SchemaChoices,
    TransformSpec,
)
from ecoscope_earthranger_io_core.query import (
    EventsQuery,
    EventTypeSchemaQuery,
    EventTypesQuery,
    ObservationsQuery,
    PatrolsQuery,
    QueryEngine,
)

from conftest import (
    get_async_patrols_rb_generator,
    get_async_rb_generator_from_storage_backend,
)

app = FastAPI()
observations = APIRouter(prefix="/observations")

# Schema for observations with patrol details - the "persisted" format from mock storage
# This matches the mock data generator output format
OBSERVATIONS_WITH_PATROL_SCHEMA_PERSISTED = pa.schema(
    [
        ("location", ga.wkb()),
        ("recorded_at", pa.string()),
        ("subject_id", pa.string()),
        ("subject_name", pa.string()),
        ("subject_subtype_id", pa.string()),
        ("source_id", pa.string()),
        ("patrol_id", pa.string()),
        ("patrol_title", pa.string()),
        ("patrol_serial_number", pa.int64()),
        ("patrol_status", pa.string()),
        ("patrol_type_value", pa.string()),
        ("patrol_type_display", pa.string()),
        ("patrol_start_time", pa.string()),
        ("patrol_end_time", pa.string()),
    ]
)


def _patrol_observations_pre_cast(earthranger_rb: pa.RecordBatch) -> pa.RecordBatch:
    """Convert an EarthRanger RecordBatch with patrol details to Ecoscope format."""
    # Add junk_status column (all False for now)
    junk_status = pa.array([False] * earthranger_rb.num_rows, type=pa.bool_())
    add_junk_status = earthranger_rb.append_column("junk_status", junk_status)

    # Rename columns to match ECOSCOPE_SLIM_V1 structure and EarthRangerIO field names
    renamed = add_junk_status.rename_columns(
        {
            "location": "geometry",
            "subject_id": "groupby_col",
            "recorded_at": "fixtime",
            "subject_name": "extra__subject__name",
            "subject_subtype_id": "extra__subject__subject_subtype",
            "source_id": "extra__source",
            "patrol_type_value": "patrol_type__value",
            "patrol_type_display": "patrol_type__display",
        }
    )

    # Add timezone to fixtime (workaround for missing +00:00 in EarthRanger data)
    fixtime_idx = renamed.schema.get_field_index("fixtime")
    fixtime_naive = renamed.column("fixtime").to_pylist()
    fixtime_utc = [t + "+00:00" if t else None for t in fixtime_naive]

    # Replace fixtime column
    result = renamed.drop_columns(["fixtime"])
    result = result.add_column(fixtime_idx, "fixtime", fixtime_utc)

    # Reorder columns to match target schema order
    target_column_order = [
        "geometry",
        "fixtime",
        "groupby_col",
        "extra__subject__name",
        "extra__subject__subject_subtype",
        "extra__source",
        "junk_status",
        "patrol_id",
        "patrol_title",
        "patrol_serial_number",
        "patrol_status",
        "patrol_type__value",
        "patrol_type__display",
        "patrol_start_time",
        "patrol_end_time",
    ]
    result = result.select(target_column_order)

    return result


# Patrol transform spec - uses the same pattern as library transforms
PATROL_TRANSFORM = TransformSpec(
    persisted_schema=OBSERVATIONS_WITH_PATROL_SCHEMA_PERSISTED,
    target_schema=OBSERVATIONS_WITH_PATROL_SCHEMA_SLIM_V1,
    pre_cast_fn=_patrol_observations_pre_cast,
)


@observations.get("/stream/arrow")
async def get_observations_streaming_arrow(
    query: ObservationsQuery = Depends(ObservationsQuery.from_query_params),
    schema: SchemaChoices = Query(
        "ECOSCOPE_SLIM_V1",
        description="Schema to use for the response",
    ),
    store_type: QueryEngine | None = Query(None),
):
    """Stream observations as an Arrow IPC stream.

    Supports both subject group observations and patrol observations:
    - Subject group observations: Use subject_ids or subject_group_name
    - Patrol observations: Use patrol_type_value, patrol_status, include_patrol_details
    """
    # When patrol details are requested, use the patrol transform
    if query.include_patrol_details:
        transform = PATROL_TRANSFORM
    else:
        transform = TRANSFORMS[schema]

    async_batch_generator = get_async_rb_generator_from_storage_backend(
        query,
        columns=transform.required_columns,
        schema=transform.pre_transform_schema,
    )
    content_stream = transform.generate_bytes(
        async_batch_generator=async_batch_generator()
    )
    try:
        return StreamingResponse(
            content_stream,
            media_type="application/vnd.apache.arrow.stream",
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to read data: {str(e)}")


app.include_router(observations)

# Patrols router
patrols = APIRouter(prefix="/patrols")


def _build_patrols_with_events_record_batch() -> pa.RecordBatch:
    """Build a canned patrols-with-events RecordBatch (one patrol, one segment,
    one event) conforming to PATROLS_WITH_EVENTS_NESTED_SCHEMA_V1."""
    event_geometry = shapely.wkb.dumps(shapely.geometry.Point(0.0, 1.0))
    event = {
        "id": "event1",
        "serial_number": 1,
        "event_type": "wildlife_sighting",
        "event_time": datetime(2015, 1, 1, 12, 0, tzinfo=timezone.utc),
        "priority": 0,
        "title": "Elephant",
        "state": "active",
        "updated_at": "2015-01-01T12:00:00+00:00",
        "created_at": "2015-01-01T12:00:00+00:00",
        "geometry": event_geometry,
        "is_collection": False,
        "event_details": '{"species": "elephant"}',
    }
    segment = {
        "id": "segment1",
        "patrol_type": "routine_patrol",
        "patrol_type_display": "Routine Patrol",
        "leader_id": "leader1",
        "time_range_start": "2015-01-01T12:00:00+00:00",
        "time_range_end": "2015-01-01T14:00:00+00:00",
        "scheduled_start": "2015-01-01T12:00:00+00:00",
        "scheduled_end": "2015-01-01T14:00:00+00:00",
        "start_location": None,
        "end_location": None,
        "events": [event],
    }
    patrol = {
        "id": "patrol1",
        "serial_number": 1000,
        "priority": 0,
        "state": "done",
        "title": "Mock Patrol 1",
        "objective": "Test objective",
        "created_at": "2015-01-01T12:00:00+00:00",
        "updated_at": "2015-01-01T12:00:00+00:00",
        "patrol_segments": [segment],
    }
    return pa.RecordBatch.from_pylist(
        [patrol], schema=PATROLS_WITH_EVENTS_NESTED_SCHEMA_V1
    )


@patrols.get("/stream/arrow")
async def get_patrols_streaming_arrow(
    query: PatrolsQuery = Depends(PatrolsQuery.from_query_params),
    store_type: QueryEngine | None = Query(None),
):
    """Stream patrols as an Arrow IPC stream."""

    async def generate_arrow_bytes():
        """Generate Arrow IPC stream bytes."""
        if query.include_events:
            sink = pa.BufferOutputStream()
            writer = pa.ipc.new_stream(sink, PATROLS_WITH_EVENTS_NESTED_SCHEMA_V1)
            try:
                writer.write_batch(_build_patrols_with_events_record_batch())
            finally:
                writer.close()
            yield sink.getvalue().to_pybytes()
            return
        async_batch_generator = get_async_patrols_rb_generator(query)
        sink = pa.BufferOutputStream()
        writer = pa.ipc.new_stream(sink, PATROLS_NESTED_SCHEMA_V1)
        try:
            async for batch in async_batch_generator():
                if batch.num_rows > 0:
                    writer.write_batch(batch)
        finally:
            writer.close()
        yield sink.getvalue().to_pybytes()

    try:
        return StreamingResponse(
            generate_arrow_bytes(),
            media_type="application/vnd.apache.arrow.stream",
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to read data: {str(e)}")


app.include_router(patrols)

# Events router
events = APIRouter(prefix="/events")


def _build_events_record_batch() -> pa.RecordBatch:
    """Build a small canned events RecordBatch conforming to EVENTS_SCHEMA_V1."""
    geometry = ga.as_wkb(["POINT (0 1)", "POINT (2 3)"])
    rows = [
        {
            "id": "event1",
            "serial_number": 1,
            "event_type_id": "et1",
            "event_type_value": "wildlife_sighting",
            "event_category_value": "monitoring",
            "title": "Elephant",
            "state": "active",
            "priority": 0,
            "event_time": datetime(2015, 1, 1, 12, 0, tzinfo=timezone.utc),
            "end_time": datetime(2015, 1, 1, 13, 0, tzinfo=timezone.utc),
            "created_at": datetime(2015, 1, 1, 12, 0, tzinfo=timezone.utc),
            "updated_at": datetime(2015, 1, 1, 12, 0, tzinfo=timezone.utc),
            "is_collection": False,
            "reported_by": {"id": "u1", "name": "Ranger A", "type": "user"},
            "event_details": '{"species": "elephant"}',
            "das_tenant_id": "tenant1",
        },
        {
            "id": "event2",
            "serial_number": 2,
            "event_type_id": "et2",
            "event_type_value": "poaching",
            "event_category_value": "security",
            "title": "Snare",
            "state": "active",
            "priority": 100,
            "event_time": datetime(2015, 2, 1, 9, 0, tzinfo=timezone.utc),
            "end_time": datetime(2015, 2, 1, 10, 0, tzinfo=timezone.utc),
            "created_at": datetime(2015, 2, 1, 9, 0, tzinfo=timezone.utc),
            "updated_at": datetime(2015, 2, 1, 9, 0, tzinfo=timezone.utc),
            "is_collection": False,
            "reported_by": {"id": "s1", "name": "Subject B", "type": "subject"},
            "event_details": '{"count": 1}',
            "das_tenant_id": "tenant1",
        },
    ]
    arrays = []
    for field in EVENTS_SCHEMA_V1:
        if field.name == "geometry":
            arrays.append(geometry)
        else:
            arrays.append(pa.array([r[field.name] for r in rows], type=field.type))
    return pa.RecordBatch.from_arrays(arrays, schema=EVENTS_SCHEMA_V1)


@events.get("/stream/arrow")
async def get_events_streaming_arrow(
    query: EventsQuery = Depends(EventsQuery.from_query_params),
    store_type: QueryEngine | None = Query(None),
):
    """Stream events as an Arrow IPC stream.

    The detail-shaping options (raw_details / parse_detail_datetimes /
    invalid_only / invalid_details / include_details) are fields on EventsQuery;
    this canned fixture accepts them via the query model and ignores them.
    """

    def generate_arrow_bytes():
        sink = pa.BufferOutputStream()
        writer = pa.ipc.new_stream(sink, EVENTS_SCHEMA_V1)
        try:
            writer.write_batch(_build_events_record_batch())
        finally:
            writer.close()
        yield sink.getvalue().to_pybytes()

    try:
        return StreamingResponse(
            generate_arrow_bytes(),
            media_type="application/vnd.apache.arrow.stream",
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to read data: {str(e)}")


def _canned_event_details_struct(parse_detail_datetimes: bool) -> pa.StructType:
    """A canned event_details struct; a date-time leaf becomes typed when the
    datetime opt-in is set (mirrors the real /events/schema behavior)."""
    when_type = pa.timestamp("ns", tz="UTC") if parse_detail_datetimes else pa.string()
    return pa.struct(
        [("species", pa.string()), ("count", pa.int64()), ("seen_at", when_type)]
    )


@events.get("/schema")
async def get_event_type_schema(
    query: EventTypeSchemaQuery = Depends(EventTypeSchemaQuery.from_query_params),
    store_type: QueryEngine | None = Query(None),
    parse_detail_datetimes: bool = Query(False),
    format: Literal["arrow", "json"] = Query("arrow"),
):
    """Return the event_details struct schema (Arrow schema message or JSON)."""
    details_struct = _canned_event_details_struct(parse_detail_datetimes)
    schema = pa.schema([("event_details", details_struct)])
    if format == "json":
        return JSONResponse({f.name: str(f.type) for f in details_struct})
    return Response(
        content=schema.serialize().to_pybytes(),
        media_type="application/vnd.apache.arrow.schema",
    )


app.include_router(events)

# Event types router
event_types = APIRouter()


def _build_event_types_record_batch() -> pa.RecordBatch:
    """Build a canned event types RecordBatch conforming to EVENT_TYPES_SCHEMA_V1."""
    rows = [
        {
            "id": "et1",
            "value": "wildlife_sighting",
            "display": "Wildlife Sighting",
            "category_value": "monitoring",
            "is_active": True,
            "is_collection": False,
        },
        {
            "id": "et2",
            "value": "poaching",
            "display": "Poaching",
            "category_value": "security",
            "is_active": True,
            "is_collection": False,
        },
    ]
    arrays = [
        pa.array([r[field.name] for r in rows], type=field.type)
        for field in EVENT_TYPES_SCHEMA_V1
    ]
    return pa.RecordBatch.from_arrays(arrays, schema=EVENT_TYPES_SCHEMA_V1)


@event_types.get("/event_types")
async def get_event_types_streaming_arrow(
    query: EventTypesQuery = Depends(EventTypesQuery.from_query_params),
    store_type: QueryEngine | None = Query(None),
):
    """Stream event types as an Arrow IPC stream."""

    def generate_arrow_bytes():
        sink = pa.BufferOutputStream()
        writer = pa.ipc.new_stream(sink, EVENT_TYPES_SCHEMA_V1)
        try:
            writer.write_batch(_build_event_types_record_batch())
        finally:
            writer.close()
        yield sink.getvalue().to_pybytes()

    try:
        return StreamingResponse(
            generate_arrow_bytes(),
            media_type="application/vnd.apache.arrow.stream",
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to read data: {str(e)}")


app.include_router(event_types)
