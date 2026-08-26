import pyarrow as pa

from ecoscope_earthranger_io_core.arrow import (
    PATROL_EVENTS_FLAT_SCHEMA_V1,
    PATROL_SEGMENT_STRUCT_V1,
    PATROL_SEGMENT_SUBJECT_STRUCT_V1,
    PATROL_SEGMENT_WITH_EVENTS_STRUCT_V1,
    PATROL_TEAM_STRUCT_V1,
    PATROL_TYPES_SCHEMA_V1,
    PATROLS_FLAT_SCHEMA_V1,
    PATROLS_NESTED_SCHEMA_V1,
    PATROLS_ONLY_SCHEMA_V1,
    PATROLS_WITH_EVENTS_NESTED_SCHEMA_V1,
)


# The six capture fields, in the order they are appended, with their types.
CAPTURE_FIELDS = [
    ("segment_details", pa.string()),
    ("type_details", pa.string()),
    ("team", PATROL_TEAM_STRUCT_V1),
    ("members", pa.list_(PATROL_SEGMENT_SUBJECT_STRUCT_V1)),
    ("assets", pa.list_(PATROL_SEGMENT_SUBJECT_STRUCT_V1)),
    ("is_pause", pa.bool_()),
]

# PATROL_SEGMENT_STRUCT_V1's fields as published before the capture fields were
# appended. Spelled out rather than sliced, so a rename or a type change to any
# of them fails here instead of silently shifting.
PRE_CAPTURE_SEGMENT_FIELDS = [
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


def _struct_names(struct: pa.StructType) -> list[str]:
    return [struct.field(i).name for i in range(struct.num_fields)]


def test_patrol_team_struct_v1_fields():
    """The team shape is das's own resolved-config team object, so a warehouse
    consumer and a das consumer describe a team the same way."""
    expected = [
        ("id", pa.string()),
        ("value", pa.string()),
        ("display", pa.string()),
        ("ordernum", pa.int64()),
        ("is_active", pa.bool_()),
    ]
    assert _struct_names(PATROL_TEAM_STRUCT_V1) == [n for n, _ in expected]
    for name, typ in expected:
        assert PATROL_TEAM_STRUCT_V1.field(name).type == typ


def test_patrol_segment_subject_struct_v1_fields():
    """Members and assets share one shape, taken from das's resolved roster
    minus ``content_type`` (a constant) and ``image_url`` (not ingested). No
    ``ordernum`` field: an Arrow list is ordered, so the link tables' ordernum
    decides list order rather than appearing in it."""
    expected = [
        ("id", pa.string()),
        ("name", pa.string()),
        ("subject_type", pa.string()),
        ("subject_subtype", pa.string()),
        ("subject_subtype_display", pa.string()),
        ("is_active", pa.bool_()),
    ]
    assert _struct_names(PATROL_SEGMENT_SUBJECT_STRUCT_V1) == [n for n, _ in expected]
    for name, typ in expected:
        assert PATROL_SEGMENT_SUBJECT_STRUCT_V1.field(name).type == typ
    for absent in ("ordernum", "content_type", "image_url"):
        assert absent not in _struct_names(PATROL_SEGMENT_SUBJECT_STRUCT_V1)


def test_patrol_segment_struct_v1_appends_capture_fields():
    """The capture fields are APPENDED to the published struct -- every
    pre-existing field keeps its name, type and position."""
    expected = PRE_CAPTURE_SEGMENT_FIELDS + CAPTURE_FIELDS
    assert _struct_names(PATROL_SEGMENT_STRUCT_V1) == [n for n, _ in expected]
    for name, typ in expected:
        assert PATROL_SEGMENT_STRUCT_V1.field(name).type == typ


def test_capture_fields_are_nullable():
    """A tenant without the patrol_schemas preview feature, and any row ingested
    before the columns existed, carries nulls. That is a normal state."""
    for name, _ in CAPTURE_FIELDS:
        assert PATROL_SEGMENT_STRUCT_V1.field(name).nullable
        assert PATROLS_FLAT_SCHEMA_V1.field(name).nullable


def test_details_columns_are_json_strings():
    """Both detail columns are JSON text here; the typed struct is derived at
    query time by the API from the relevant schema document, as event_details is."""
    assert PATROL_SEGMENT_STRUCT_V1.field("segment_details").type == pa.string()
    assert PATROL_SEGMENT_STRUCT_V1.field("type_details").type == pa.string()


def test_patrols_flat_schema_v1_appends_capture_columns():
    """The flat schema gains the same six as top-level columns, appended after
    the existing segment columns."""
    expected = [
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
    ] + CAPTURE_FIELDS
    assert PATROLS_FLAT_SCHEMA_V1.names == [n for n, _ in expected]
    for name, typ in expected:
        assert PATROLS_FLAT_SCHEMA_V1.field(name).type == typ


def test_embedding_schemas_pick_up_the_capture_fields():
    """The nested schemas embed PATROL_SEGMENT_STRUCT_V1 by reference, so they
    inherit the capture fields rather than restating them."""
    for schema in (PATROLS_NESTED_SCHEMA_V1, PATROLS_WITH_EVENTS_NESTED_SCHEMA_V1):
        segment_type = schema.field("patrol_segments").type.value_type
        for name, typ in CAPTURE_FIELDS:
            assert segment_type.field(name).type == typ


def test_patrol_segment_with_events_struct_keeps_leader_name_and_events_last():
    """The with-events struct is the segment struct + leader_name + events. The
    capture fields land inside the segment-struct prefix, so leader_name and
    events stay trailing."""
    assert _struct_names(PATROL_SEGMENT_WITH_EVENTS_STRUCT_V1) == _struct_names(
        PATROL_SEGMENT_STRUCT_V1
    ) + ["leader_name", "events"]


def test_patrol_events_flat_schema_v1_appends_capture_columns():
    """The one-row-per-event flat schema carries its segment's capture fields as
    context, appended after the existing patrol/segment context columns."""
    assert PATROL_EVENTS_FLAT_SCHEMA_V1.names[-6:] == [n for n, _ in CAPTURE_FIELDS]
    for name, typ in CAPTURE_FIELDS:
        assert PATROL_EVENTS_FLAT_SCHEMA_V1.field(name).type == typ
    # The pre-existing columns are untouched, in order.
    assert PATROL_EVENTS_FLAT_SCHEMA_V1.names[:17] == [
        "id",
        "serial_number",
        "event_type",
        "event_time",
        "priority",
        "title",
        "state",
        "updated_at",
        "created_at",
        "geometry",
        "is_collection",
        "event_details",
        "patrol_id",
        "patrol_serial_number",
        "patrol_segment_id",
        "patrol_type",
        "patrol_start_time",
    ]


def test_patrol_events_flat_schema_tolerates_rows_missing_capture_keys():
    """``get_patrol_events`` builds against this constant with
    ``pa.Table.from_pylist``. Appending columns must not break a row dict that
    predates them -- from_pylist fills an absent key with null."""
    table = pa.Table.from_pylist(
        [{"id": "e1", "patrol_id": "p1"}], schema=PATROL_EVENTS_FLAT_SCHEMA_V1
    )
    row = table.to_pylist()[0]
    assert row["id"] == "e1"
    for name, _ in CAPTURE_FIELDS:
        assert row[name] is None


def test_patrols_only_schema_v1_is_unaffected():
    """PATROLS_ONLY_SCHEMA_V1 has no segment columns, so it carries no capture
    fields either."""
    assert PATROLS_ONLY_SCHEMA_V1.names == [
        "id",
        "serial_number",
        "priority",
        "state",
        "title",
        "objective",
        "created_at",
        "updated_at",
    ]
    for name, _ in CAPTURE_FIELDS:
        assert name not in PATROLS_ONLY_SCHEMA_V1.names


def test_no_v2_patrol_schema_exists():
    """The capture fields were appended to the published _V1 schemas rather than
    minting _V2 variants; guard against one creeping back in."""
    import ecoscope_earthranger_io_core.arrow as arrow_module

    assert not [n for n in dir(arrow_module) if n.startswith("PATROL") and "_V2" in n]


def test_patrol_types_schema_v1_fields():
    """Lock the /patrol_types listing contract (patrol-type display-name
    resolution), mirroring EVENT_TYPES_SCHEMA_V1."""
    expected = [
        ("id", pa.string()),
        ("value", pa.string()),
        ("display", pa.string()),
        ("ordernum", pa.int64()),
        ("is_active", pa.bool_()),
    ]
    assert PATROL_TYPES_SCHEMA_V1.names == [n for n, _ in expected]
    for name, typ in expected:
        assert PATROL_TYPES_SCHEMA_V1.field(name).type == typ


def test_patrol_types_schema_v1_does_not_carry_the_schema_document():
    """The schema document is served as Arrow IPC schema bytes or a JSON mapping
    by the schema endpoints, never as a column of the listing -- same rule as
    EVENT_TYPES_SCHEMA_V1."""
    assert "schema" not in PATROL_TYPES_SCHEMA_V1.names


def test_capture_fields_round_trip_populated_values():
    """The resolved structs carry what a consumer needs without a das call: a
    team's display name, and a member's name and type."""
    row = {
        "id": "e1",
        "patrol_id": "p1",
        "segment_details": '{"objective": "north fence"}',
        "type_details": "{}",
        "team": {
            "id": "t1",
            "value": "alpha",
            "display": "Alpha",
            "ordernum": 1,
            "is_active": True,
        },
        "members": [
            {
                "id": "m1",
                "name": "Ranger Ali",
                "subject_type": "person",
                "subject_subtype": "ranger",
                "subject_subtype_display": "Ranger",
                "is_active": True,
            }
        ],
        "assets": [],
        "is_pause": False,
    }
    out = pa.Table.from_pylist([row], schema=PATROL_EVENTS_FLAT_SCHEMA_V1).to_pylist()[
        0
    ]
    assert out["team"]["display"] == "Alpha"
    assert out["members"][0]["subject_type"] == "person"
    # An empty roster is [] and a non-pause is False -- distinct from the null
    # that means "not captured". Both survive the round trip.
    assert out["assets"] == []
    assert out["is_pause"] is False
    assert out["type_details"] == "{}"


def test_member_order_survives_an_ipc_round_trip():
    """Why PATROL_SEGMENT_SUBJECT_STRUCT_V1 carries no ``ordernum``: an Arrow
    list is ordered by construction, so the order the API applies from the link
    tables' ordernum reaches the client intact through the IPC stream the client
    reads with ``pa.ipc.open_stream``."""
    members = [
        {
            "id": str(i),
            "name": f"member-{i}",
            "subject_type": "person",
            "subject_subtype": None,
            "subject_subtype_display": None,
            "is_active": True,
        }
        for i in range(5)
    ]
    table = pa.Table.from_pylist(
        [{"id": "p1", "patrol_segments": [{"id": "s1", "members": members}]}],
        schema=PATROLS_WITH_EVENTS_NESTED_SCHEMA_V1,
    )
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    back = pa.ipc.open_stream(sink.getvalue()).read_all()
    read_members = back.to_pylist()[0]["patrol_segments"][0]["members"]
    assert [m["id"] for m in read_members] == ["0", "1", "2", "3", "4"]
