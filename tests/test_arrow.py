import io
from datetime import datetime

import geoarrow.pyarrow as ga  # type: ignore[import-untyped]
import pyarrow as pa
import pytest
from ecoscope_earthranger_io_core.arrow import (
    OBSERVATIONS_SCHEMA__ECOSCOPE_SLIM_V1,
    OBSERVATIONS_WITH_PATROL_SCHEMA_SLIM_V1,
    SchemaChoices,
    TRANSFORMS,
)
from ecoscope_earthranger_io_core.query import ObservationsQuery

from conftest import get_async_rb_generator_from_storage_backend


@pytest.mark.parametrize(
    "schema",
    [
        OBSERVATIONS_SCHEMA__ECOSCOPE_SLIM_V1,
        OBSERVATIONS_WITH_PATROL_SCHEMA_SLIM_V1,
    ],
)
def test_slim_schemas_geometry_has_epsg_4326_crs(schema: pa.Schema) -> None:
    # Extension type `==` ignores CRS; assert CRS explicitly.
    expected_geometry_type = ga.wkb().with_crs("EPSG:4326")
    geom_type = schema.field("geometry").type
    assert geom_type.crs == expected_geometry_type.crs


@pytest.mark.asyncio
async def test_generate_bytes():
    transform = TRANSFORMS[SchemaChoices.ECOSCOPE_SLIM_V1]
    query = ObservationsQuery(
        tenant_domain="some-site.pamdas.org",
        subject_ids=["subject1", "subject2"],
        range_start=datetime(2023, 1, 1),
        range_end=datetime(2023, 12, 31),
    )
    async_batch_generator = get_async_rb_generator_from_storage_backend(
        query,
        columns=transform.required_columns,
        schema=transform.pre_transform_schema,
    )
    content_stream = transform.generate_bytes(
        async_batch_generator=async_batch_generator()
    )
    sink = io.BytesIO()
    async for chunk in content_stream:
        sink.write(chunk)
    sink.seek(0)
    source = sink.getvalue()
    table = pa.ipc.open_stream(source).read_all()
    assert table.num_rows > 0
