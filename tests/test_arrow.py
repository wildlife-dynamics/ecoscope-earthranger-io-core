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


def test_slim_schema_carries_the_subject_id_under_the_earthranger_name():
    """ERDW-268: the subject id ships as `extra__subject__id` on top of `groupby_col`,
    becoming `subject__id` once ecoscope strips the prefix."""
    assert OBSERVATIONS_SCHEMA__ECOSCOPE_SLIM_V1.field("extra__subject__id").type == (
        pa.string()
    )
    # `_observations_pre_cast` inserts positionally and the cast is order-sensitive,
    # so pin the whole stream schema, in order.
    assert OBSERVATIONS_SCHEMA__ECOSCOPE_SLIM_V1.names == [
        "geometry",
        "fixtime",
        "groupby_col",
        "extra__subject__id",
        "extra__subject__name",
        "extra__subject__subject_subtype",
        "extra__subject__additional",
        "extra__source",
        "junk_status",
    ]


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
        (None, None),  # {"rgb": null} -- key present, no value
    ],
)
def test_documents_earthranger_rgb_format(rgb, expected_rgba):
    """Executable documentation of the `rgb` format this column transports.

    This asserts nothing about io-core -- the transform is a verbatim passthrough,
    already pinned by `test_slim_transform_round_trips_subject_additional`. It exists
    to record which values the downstream consumer can use, mirroring `parse_rgb_str`
    in ecoscope.platform.tasks.transformation._subjects (copied, not imported: io-core
    must not depend on ecoscope). If that function changes, this will NOT detect the
    drift -- the ecoscope side owns that test.
    """

    def parse_rgb_str(rgb_str):
        try:
            r, g, b = [int(x.strip()) for x in rgb_str.split(",")]
            return (r / 255.0, g / 255.0, b / 255.0, 1.0)
        except (ValueError, AttributeError, TypeError):
            return None

    assert parse_rgb_str(rgb) == expected_rgba


@pytest.mark.parametrize(
    "additional",
    [
        pytest.param(json.dumps({"rgb": None}), id="rgb-key-null"),
        pytest.param(json.dumps({"sex": "female"}), id="no-rgb-key"),
    ],
)
def test_subject_without_a_colour_still_transports(additional):
    """The common real-world case: a subject has `additional` but no usable colour.
    io-core must transport it unchanged and leave the fallback decision to the
    consumer (ecoscope guards with `if rgb_value:`)."""
    transform, rb = _observations_pre_transform_batch(additional)

    out = transform.transform(rb)

    assert out.column("extra__subject__additional")[0].as_py() == additional
    assert "rgb" not in json.loads(additional) or json.loads(additional)["rgb"] is None


def test_slim_transform_populates_subject_id_from_the_same_source_as_groupby_col():
    """Both columns come from the store's single `subject_id` projection, so they must
    agree row for row. Multiple distinct subjects, so an insert that picked up the
    wrong column cannot pass by coincidence."""
    transform = TRANSFORMS[SchemaChoices.ECOSCOPE_SLIM_V1]
    subject_ids = ["subject1", "subject2", "subject1"]
    rb = pa.record_batch(
        {
            "location": ga.array(
                [ga.as_wkb(["POINT (37.5 -2.5)"])[0].wkb] * len(subject_ids)
            ),
            "recorded_at": ["2015-01-01T00:00:00"] * len(subject_ids),
            "subject_id": subject_ids,
            "subject_name": ["eco_1", "eco_2", "eco_1"],
            "subject_subtype_id": ["elephant"] * len(subject_ids),
            "subject_additional": [SUBJECT_ADDITIONAL_JSON] * len(subject_ids),
            "source_id": ["source1", "source2", "source1"],
        },
        schema=transform.pre_transform_schema,
    )

    out = transform.transform(rb)

    assert out.schema.equals(transform.stream_schema)
    assert out.column("extra__subject__id").to_pylist() == subject_ids
    assert (
        out.column("extra__subject__id").to_pylist()
        == out.column("groupby_col").to_pylist()
    )


def test_slim_transform_emits_subject_id_for_an_empty_batch():
    """Empty batches are the normal tail of a streaming response, and the new insert
    has to survive one (a 0-length column, then a cast to the target schema)."""
    transform = TRANSFORMS[SchemaChoices.ECOSCOPE_SLIM_V1]
    rb = pa.record_batch(
        {name: [] for name in transform.pre_transform_schema.names},
        schema=transform.pre_transform_schema,
    )

    out = transform.transform(rb)

    assert out.num_rows == 0
    assert out.schema.equals(transform.stream_schema)


def test_slim_pre_transform_schema_is_the_store_projection_contract():
    """`pre_transform_schema` is handed to the stores as their SELECT list, and
    `RecordBatch.cast` is order-sensitive -- so reordering
    OBSERVATIONS_SCHEMA__EARTHRANGER_FULL_V1 silently breaks the slim cast at request
    time. Pin the exact projection the stores must emit, in order."""
    assert TRANSFORMS[SchemaChoices.ECOSCOPE_SLIM_V1].pre_transform_schema.names == [
        "location",
        "recorded_at",
        "subject_id",
        "subject_name",
        "subject_subtype_id",
        "subject_additional",
        "source_id",
    ]


def test_slim_transform_requires_subject_additional_column():
    """A store that does not project `subject_additional` must fail loudly rather
    than silently dropping the column (stores do `batch.select(schema.names)`, so in
    practice they raise before reaching the transform)."""
    transform = TRANSFORMS[SchemaChoices.ECOSCOPE_SLIM_V1]
    without = transform.pre_transform_schema.remove(
        transform.pre_transform_schema.get_field_index("subject_additional")
    )
    rb = pa.record_batch(
        {
            "location": ga.array([ga.as_wkb(["POINT (37.5 -2.5)"])[0].wkb]),
            "recorded_at": ["2015-01-01T00:00:00"],
            "subject_id": ["subject1"],
            "subject_name": ["eco_1"],
            "subject_subtype_id": ["elephant"],
            "source_id": ["source1"],
        },
        schema=without,
    )

    with pytest.raises(ValueError, match="field names are not matching"):
        transform.transform(rb)


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


@pytest.mark.asyncio
async def test_generate_bytes_includes_subject_id():
    """ERDW-268 over the streaming path the API actually serves: the column must
    survive IPC, populated, and still agree with `groupby_col`."""
    transform = TRANSFORMS[SchemaChoices.ECOSCOPE_SLIM_V1]
    query = ObservationsQuery(
        tenant_domain="some-site.pamdas.org",
        subject_ids=["subject1", "subject2"],
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

    subject_ids = table.column("extra__subject__id").to_pylist()
    assert subject_ids
    assert all(v is not None for v in subject_ids)
    assert subject_ids == table.column("groupby_col").to_pylist()
