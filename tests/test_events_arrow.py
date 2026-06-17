import geoarrow.pyarrow as ga  # type: ignore[import-untyped]
import pyarrow as pa

from ecoscope_earthranger_io_core.arrow import (
    EVENTS_SCHEMA_V1,
    PATROL_EVENT_STRUCT_V1,
    PATROLS_NESTED_SCHEMA_V1,
    PATROLS_WITH_EVENTS_NESTED_SCHEMA_V1,
    REPORTED_BY_STRUCT_V1,
    SchemaChoices,
    TRANSFORMS,
)


def test_events_schema_v1_fields():
    """Lock the flat events schema field names + types (the wire contract)."""
    expected = [
        ("id", pa.string()),
        ("serial_number", pa.int64()),
        ("event_type_id", pa.string()),
        ("event_type_value", pa.string()),
        ("event_category_value", pa.string()),
        ("title", pa.string()),
        ("state", pa.string()),
        ("priority", pa.int64()),
        ("event_time", pa.string()),
        ("end_time", pa.string()),
        ("created_at", pa.string()),
        ("updated_at", pa.string()),
        ("is_collection", pa.bool_()),
        ("geometry", ga.wkb().with_crs("EPSG:4326")),
        ("reported_by", REPORTED_BY_STRUCT_V1),
        ("event_details", pa.string()),
        ("das_tenant_id", pa.string()),
    ]
    assert EVENTS_SCHEMA_V1.names == [name for name, _ in expected]
    for name, typ in expected:
        assert EVENTS_SCHEMA_V1.field(name).type == typ


def test_reported_by_struct_v1_fields():
    """reported_by is a uniform {id, name, type} struct for both subject and
    user reporters -- the subject/user branch is collapsed server-side, so the
    shape never diverges (type discriminates, no union needed)."""
    expected = [
        ("id", pa.string()),
        ("name", pa.string()),
        ("type", pa.string()),
    ]
    assert [
        REPORTED_BY_STRUCT_V1.field(i).name
        for i in range(REPORTED_BY_STRUCT_V1.num_fields)
    ] == [n for n, _ in expected]
    for name, typ in expected:
        assert REPORTED_BY_STRUCT_V1.field(name).type == typ
    assert EVENTS_SCHEMA_V1.field("reported_by").type == REPORTED_BY_STRUCT_V1


def test_events_details_is_string_on_flat_schema():
    """event_details is a JSON string on the flat schema; the typed struct is
    swapped in dynamically by the API, not here."""
    assert EVENTS_SCHEMA_V1.field("event_details").type == pa.string()


def test_patrol_event_struct_v1_fields():
    expected = [
        ("id", pa.string()),
        ("serial_number", pa.int64()),
        ("event_type", pa.string()),
        ("priority", pa.int64()),
        ("title", pa.string()),
        ("state", pa.string()),
        ("updated_at", pa.string()),
        ("created_at", pa.string()),
        ("geometry", pa.binary()),
        ("is_collection", pa.bool_()),
        ("event_details", pa.string()),
    ]
    assert [
        PATROL_EVENT_STRUCT_V1.field(i).name
        for i in range(PATROL_EVENT_STRUCT_V1.num_fields)
    ] == [n for n, _ in expected]
    for name, typ in expected:
        assert PATROL_EVENT_STRUCT_V1.field(name).type == typ


def test_patrols_nested_schema_v1_unchanged():
    """Regression guard: the published PATROLS_NESTED_SCHEMA_V1 must NOT gain an
    events column (never mutate a published versioned schema)."""
    assert PATROLS_NESTED_SCHEMA_V1.names == [
        "id",
        "serial_number",
        "priority",
        "state",
        "title",
        "objective",
        "created_at",
        "updated_at",
        "patrol_segments",
    ]
    assert "events" not in PATROLS_NESTED_SCHEMA_V1.names


def test_patrols_with_events_is_nested_plus_events_list():
    """The with-events schema = the nested schema's fields + a trailing events
    list of PATROL_EVENT_STRUCT_V1."""
    assert (
        PATROLS_WITH_EVENTS_NESTED_SCHEMA_V1.names
        == PATROLS_NESTED_SCHEMA_V1.names + ["events"]
    )
    # the shared fields are identical types
    for name in PATROLS_NESTED_SCHEMA_V1.names:
        assert (
            PATROLS_WITH_EVENTS_NESTED_SCHEMA_V1.field(name).type
            == PATROLS_NESTED_SCHEMA_V1.field(name).type
        )
    assert PATROLS_WITH_EVENTS_NESTED_SCHEMA_V1.field("events").type == pa.list_(
        PATROL_EVENT_STRUCT_V1
    )


def test_transforms_event_entries_resolve():
    """The new SchemaChoices have passthrough TransformSpec entries whose
    stream_schema is the flat / with-events schema."""
    events_spec = TRANSFORMS[SchemaChoices.EVENTS_FLAT_V1]
    assert events_spec.target_schema is None  # passthrough
    assert events_spec.persisted_schema == EVENTS_SCHEMA_V1
    assert events_spec.stream_schema == EVENTS_SCHEMA_V1  # persisted == streamed

    patrols_spec = TRANSFORMS[SchemaChoices.PATROLS_WITH_EVENTS_NESTED_V1]
    assert patrols_spec.target_schema is None
    assert patrols_spec.persisted_schema == PATROLS_WITH_EVENTS_NESTED_SCHEMA_V1
    assert patrols_spec.stream_schema == PATROLS_WITH_EVENTS_NESTED_SCHEMA_V1
