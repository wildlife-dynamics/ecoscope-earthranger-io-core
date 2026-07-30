import io
import json
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


def test_slim_schemas_include_extra_source():
    """extra__source must be in the slim schemas so downstream can access it."""
    assert "extra__source" in OBSERVATIONS_SCHEMA__ECOSCOPE_SLIM_V1.names
    assert "extra__source" in OBSERVATIONS_WITH_PATROL_SCHEMA_SLIM_V1.names


def test_slim_schema_includes_subject_additional_as_nullable_string():
    """The subject `additional` JSON is carried verbatim as a string (EarthRanger
    declares no schema for it). It is always present so the stream schema is stable,
    and null unless the query sets `include_subject_additional`."""
    field = OBSERVATIONS_SCHEMA__ECOSCOPE_SLIM_V1.field("extra__subject__additional")
    assert field.type == pa.string()
    assert field.nullable


def _observations_pre_transform_batch(subject_additional):
    """One-row batch shaped like the store's SELECT for the ECOSCOPE_SLIM_V1 transform."""
    transform = TRANSFORMS[SchemaChoices.ECOSCOPE_SLIM_V1]
    return transform, pa.record_batch(
        {
            "location": ga.array([ga.as_wkb(["POINT (37.5 -2.5)"])[0].wkb]),
            "recorded_at": ["2015-01-01T00:00:00"],
            "subject_id": ["subject1"],
            "subject_name": ["eco_1"],
            "subject_subtype_id": ["elephant"],
            "subject_additional": [subject_additional],
            "source_id": ["source1"],
        },
        schema=transform.pre_transform_schema,
    )


# A realistic subject `additional` payload. EarthRanger writes `rgb` as a
# comma-separated decimal triple -- `','.join(str(random.randint(0, 255)) ...)` in
# das/observations/admin.py, with DEFAULT_COLOR = '255,255,0' -- alongside the keys
# its serializer flattens (sex/region/country/species) and admin-form extras.
# NOTE: `rgb` is NOT hex. Hex is only the format of ecoscope's `default_color`
# parameter, parsed by a different function; a hex value here fails parse_rgb_str
# and silently falls back to the default colour.
SUBJECT_ADDITIONAL_JSON = json.dumps(
    {
        "rgb": "14,203,87",
        "sex": "female",
        "region": "Mara",
        "country": "Kenya",
        "species": "elephant",
        "tm_animal_id": "A-1",
    }
)


@pytest.mark.parametrize(
    "subject_additional",
    [
        pytest.param(SUBJECT_ADDITIONAL_JSON, id="realistic-payload"),
        pytest.param(json.dumps({"rgb": "255,255,0"}), id="earthranger-default-color"),
        pytest.param(json.dumps({"rgb": "255, 0, 0"}), id="whitespace-tolerated"),
        pytest.param(json.dumps({}), id="empty-additional"),
        pytest.param(None, id="omitted-null"),
    ],
)
def test_slim_transform_round_trips_subject_additional(subject_additional):
    """The JSON must survive the transform byte-for-byte, in the same stream schema,
    for both the opt-in and omitted (null) cases. It is carried verbatim -- no
    parsing, reformatting, or key filtering happens here."""
    transform, rb = _observations_pre_transform_batch(subject_additional)

    out = transform.transform(rb)

    assert out.schema.equals(transform.stream_schema)
    assert out.column("extra__subject__additional")[0].as_py() == subject_additional


@pytest.mark.parametrize(
    "rgb, expected_rgba",
    [
        ("14,203,87", (14 / 255.0, 203 / 255.0, 87 / 255.0, 1.0)),
        ("255,255,0", (1.0, 1.0, 0.0, 1.0)),  # EarthRanger's DEFAULT_COLOR
        ("255, 0, 0", (1.0, 0.0, 0.0, 1.0)),  # ecoscope strips whitespace
        ("#FFFF00", None),  # hex is NOT the additional['rgb'] format
        ("not-a-colour", None),
    ],
)
def test_subject_additional_rgb_is_consumable_by_ecoscope(rgb, expected_rgba):
    """Contract check on the value we transport: after the round trip, the `rgb` key
    must still parse under ecoscope's rules. Replicates `parse_rgb_str` from
    ecoscope.platform.tasks.transformation._subjects rather than importing it, since
    io-core must not depend on ecoscope."""

    def parse_rgb_str(rgb_str):
        try:
            r, g, b = [int(x.strip()) for x in rgb_str.split(",")]
            return (r / 255.0, g / 255.0, b / 255.0, 1.0)
        except (ValueError, AttributeError):
            return None

    transform, rb = _observations_pre_transform_batch(json.dumps({"rgb": rgb}))

    out = transform.transform(rb)
    transported = json.loads(out.column("extra__subject__additional")[0].as_py())

    assert parse_rgb_str(transported["rgb"]) == expected_rgba


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


@pytest.mark.asyncio
async def test_generate_bytes_includes_extra_source():
    """The ECOSCOPE_SLIM_V1 transform must produce an extra__source column."""
    transform = TRANSFORMS[SchemaChoices.ECOSCOPE_SLIM_V1]
    query = ObservationsQuery(
        tenant_domain="some-site.pamdas.org",
        subject_ids=["subject1"],
        range_start=datetime(2023, 1, 1),
        range_end=datetime(2023, 1, 2),
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
    table = pa.ipc.open_stream(sink).read_all()
    assert "extra__source" in table.schema.names
    source_values = table.column("extra__source").to_pylist()
    assert all(v is not None for v in source_values)
