import geoarrow.pyarrow  # type: ignore[import-untyped]
import pyarrow as pa


OBSERVATIONS_EARTHRANGER_ARROW_SCHEMA = pa.schema(
    [
        ("location", geoarrow.pyarrow.wkb()),
        ("recorded_at", pa.string()),
        ("subject_id", pa.string()),
        ("subject_name", pa.string()),
        ("subject_subtype_id", pa.string()),
    ]
)
OBSERVATIONS_ECOSCOPE_ARROW_SCHEMA = pa.schema(
    [
        ("geometry", geoarrow.pyarrow.wkb()),
        ("fixtime", pa.string()),
        ("groupby_col", pa.string()),
        ("extra__subject__name", pa.string()),
        ("extra__subject__subject_subtype", pa.string()),
    ]
)


def to_ecoscope_schema(earthranger_rb: pa.RecordBatch) -> pa.RecordBatch:
    """Convert an EarthRanger RecordBatch to an Ecoscope RecordBatch."""
    assert earthranger_rb.schema.equals(OBSERVATIONS_EARTHRANGER_ARROW_SCHEMA), (
        f"Expected input schema to be:\n {OBSERVATIONS_EARTHRANGER_ARROW_SCHEMA}\n "
        f"but got:\n {earthranger_rb.schema}"
    )
    renamed_columns = earthranger_rb.rename_columns(
        {
            "location": "geometry",
            "subject_id": "groupby_col",
            "recorded_at": "fixtime",
            "subject_name": "extra__subject__name",
            "subject_subtype_id": "extra__subject__subject_subtype",
        }
    )
    return renamed_columns.cast(OBSERVATIONS_ECOSCOPE_ARROW_SCHEMA)
