from dataclasses import dataclass
from typing import Callable

import geoarrow.pyarrow  # type: ignore[import-untyped]
import pyarrow as pa
import pyarrow.compute as pc


@dataclass(frozen=True)
class SchemaConversion:
    """Template algorithm for RecordBatch schema conversion."""

    source_schema: pa.Schema
    target_schema: pa.Schema
    pre_cast_fn: Callable[[pa.RecordBatch], pa.RecordBatch] | None = None
    post_cast_fn: Callable[[pa.RecordBatch], pa.RecordBatch] | None = None

    def convert(self, source_rb: pa.RecordBatch) -> pa.RecordBatch:
        """Convert a source RecordBatch (`source_rb`) to a RecordBatch
        with the target schema.
        """
        assert source_rb.schema.equals(self.source_schema), (
            f"Expected input schema to be:\n\n{self.source_schema}\n\n"
            f"but got:\n\n{source_rb.schema}\n\n"
        )
        if self.pre_cast_fn:
            source_rb = self.pre_cast_fn(source_rb)
        target_rb = source_rb.cast(self.target_schema)
        if self.post_cast_fn:
            target_rb = self.post_cast_fn(target_rb)
        return target_rb


OBSERVATIONS_SCHEMA__EARTHRANGER_FULL_V1 = pa.schema(
    [
        ("created_at", pa.timestamp("ns")),
        ("exclusion_flags", pa.string()),
        ("is_active", pa.bool_()),
        ("location", geoarrow.pyarrow.wkb()),
        ("manufacturer_id", pa.string()),
        ("recorded_at", pa.timestamp("ns")),
        ("subject_id", pa.string()),
        ("subject_name", pa.string()),
        ("subject_subtype_id", pa.string()),
        ("das_tenant_id", pa.string()),
        ("domain", pa.string()),
        ("observation_id", pa.string()),
        ("source_id", pa.string()),
    ],
)
OBSERVATIONS_SCHEMA__EARTHRANGER_SLIM_V1 = pa.schema(
    [
        ("location", geoarrow.pyarrow.wkb()),
        ("recorded_at", pa.string()),
        ("subject_id", pa.string()),
        ("subject_name", pa.string()),
        ("subject_subtype_id", pa.string()),
    ]
)
OBSERVATIONS_SCHEMA__ECOSCOPE_SLIM_V1 = pa.schema(
    [
        ("geometry", geoarrow.pyarrow.wkb()),
        ("fixtime", pa.timestamp("ns")),
        ("groupby_col", pa.string()),
        ("extra__subject__name", pa.string()),
        ("extra__subject__subject_subtype", pa.string()),
        ("junk_status", pa.bool_()),
    ]
)


def _observations_pre_cast(earthranger_rb: pa.RecordBatch) -> pa.RecordBatch:
    """Convert an EarthRanger RecordBatch to an Ecoscope RecordBatch."""

    junk_status = pa.array([False] * earthranger_rb.num_rows, type=pa.bool_())
    add_junk_status = earthranger_rb.append_column("junk_status", junk_status)
    return add_junk_status.rename_columns(
        {
            "location": "geometry",
            "subject_id": "groupby_col",
            "recorded_at": "fixtime",
            "subject_name": "extra__subject__name",
            "subject_subtype_id": "extra__subject__subject_subtype",
        }
    )


def _observations_post_cast(ecoscope_rb: pa.RecordBatch) -> pa.RecordBatch:
    # NOTE: workaround for missing +00:00 timezone offset in EarthRanger data, can be removed
    # once EarthRanger data is fixed to include timezone offsets.
    fixtime_naive = ecoscope_rb.column("fixtime")
    fixtime_utc = pc.assume_timezone(fixtime_naive, timezone="UTC")  # type: ignore[call-overload]
    return ecoscope_rb.drop_columns("fixtime").append_column("fixtime", fixtime_utc)


OBSERVATIONS_CONVERSION__EARTHRANGER_SLIM_V1__ECOSCOPE_SLIM_V1 = SchemaConversion(
    source_schema=OBSERVATIONS_SCHEMA__EARTHRANGER_SLIM_V1,
    target_schema=OBSERVATIONS_SCHEMA__ECOSCOPE_SLIM_V1,
    pre_cast_fn=_observations_pre_cast,
    post_cast_fn=_observations_post_cast,
)
