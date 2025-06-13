import geoarrow.pyarrow  # type: ignore[import-untyped]
import pyarrow as pa


OBSERVATIONS_ARROW_SCHEMA = pa.schema(
    [
        ("location", geoarrow.pyarrow.wkb()),
        ("recorded_at", pa.string()),
        ("subject_id", pa.string()),
        ("subject_name", pa.string()),
        ("subject_subtype_id", pa.string()),
    ]
)
