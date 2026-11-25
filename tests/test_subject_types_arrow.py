import pyarrow as pa

from ecoscope_earthranger_io_core.arrow import (
    SUBJECT_SUBTYPE_STRUCT_V1,
    SUBJECT_SUBTYPES_SCHEMA_V1,
    SUBJECT_TYPES_SCHEMA_V1,
    SUBJECT_TYPES_WITH_SUBTYPES_SCHEMA_V1,
)

SUBTYPE_FIELDS = [
    ("id", pa.string()),
    ("value", pa.string()),
    ("display", pa.string()),
    ("subject_type", pa.string()),
    ("subject_type_display", pa.string()),
    ("ordernum", pa.float64()),
    ("is_active", pa.bool_()),
    ("has_subjects_assigned", pa.bool_()),
]

TYPE_FIELDS = [
    ("id", pa.string()),
    ("value", pa.string()),
    ("display", pa.string()),
    ("ordernum", pa.float64()),
    ("is_active", pa.bool_()),
]


def test_subject_subtype_struct_v1_fields():
    assert [(f.name, f.type) for f in SUBJECT_SUBTYPE_STRUCT_V1] == SUBTYPE_FIELDS


def test_subject_subtypes_schema_v1_matches_the_struct():
    assert [(f.name, f.type) for f in SUBJECT_SUBTYPES_SCHEMA_V1] == SUBTYPE_FIELDS


def test_subject_types_schema_v1_fields():
    assert [(f.name, f.type) for f in SUBJECT_TYPES_SCHEMA_V1] == TYPE_FIELDS


def test_subject_types_with_subtypes_schema_v1_fields():
    assert [(f.name, f.type) for f in SUBJECT_TYPES_WITH_SUBTYPES_SCHEMA_V1] == [
        *TYPE_FIELDS,
        ("subtypes", pa.list_(SUBJECT_SUBTYPE_STRUCT_V1)),
    ]


def test_subject_type_schemas_do_not_carry_a_schema_document():
    for schema in (
        SUBJECT_TYPES_SCHEMA_V1,
        SUBJECT_TYPES_WITH_SUBTYPES_SCHEMA_V1,
        SUBJECT_SUBTYPES_SCHEMA_V1,
    ):
        assert "schema" not in schema.names


def test_subject_types_with_subtypes_round_trip_through_ipc():
    rows = [
        {
            "id": "t1",
            "value": "wildlife",
            "display": "Wildlife",
            "ordernum": 1.5,
            "is_active": None,
            "subtypes": [
                {
                    "id": "s1",
                    "value": "elephant",
                    "display": "Elephant",
                    "subject_type": "wildlife",
                    "subject_type_display": "Wildlife",
                    "ordernum": None,
                    "is_active": True,
                    "has_subjects_assigned": False,
                }
            ],
        },
        {
            "id": "t2",
            "value": "vehicle",
            "display": "Vehicle",
            "ordernum": 2.0,
            "is_active": True,
            "subtypes": [],
        },
    ]
    batch = pa.RecordBatch.from_pylist(
        rows, schema=SUBJECT_TYPES_WITH_SUBTYPES_SCHEMA_V1
    )
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, SUBJECT_TYPES_WITH_SUBTYPES_SCHEMA_V1) as writer:
        writer.write_batch(batch)
    table = pa.ipc.open_stream(sink.getvalue()).read_all()
    assert table.schema.equals(SUBJECT_TYPES_WITH_SUBTYPES_SCHEMA_V1)
    assert table.to_pylist() == rows
