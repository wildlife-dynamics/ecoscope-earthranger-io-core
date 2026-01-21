import geoarrow.pyarrow as ga  # type: ignore[import-untyped]
import pyarrow as pa
from fastapi import APIRouter, Depends, FastAPI, HTTPException, Query
from fastapi.responses import StreamingResponse

from ecoscope_earthranger_io_core.arrow import (
    OBSERVATIONS_WITH_PATROL_SCHEMA_SLIM_V1,
    TRANSFORMS,
    SchemaChoices,
    TransformSpec,
)
from ecoscope_earthranger_io_core.query import ObservationsQuery

from conftest import get_async_rb_generator_from_storage_backend

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
