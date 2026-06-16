"""Tests for ecoscope_earthranger_io_core.events (schema translation/coercion).

Covers the JSON-Schema -> Arrow translation and value coercion, plus the
datetime opt-in (parse_datetimes) and details_invalid_mask.
"""

import json
from datetime import date, datetime, timezone

import pyarrow as pa
import pytest

from ecoscope_earthranger_io_core.events import (
    build_details_struct,
    coerce_details_to_struct,
    details_invalid_mask,
    event_details_schema,
    json_schema_to_arrow_struct,
)


def _schema_text_v1(properties: dict) -> str:
    """Wrap ``properties`` in the legacy envelope (``.schema.properties``)."""
    return json.dumps(
        {
            "schema": {
                "$schema": "http://json-schema.org/draft-04/schema#",
                "type": "object",
                "properties": properties,
            },
            "definition": [],
        }
    )


def _schema_text_v2(properties: dict) -> str:
    """Wrap ``properties`` in the modern form envelope (``.json.properties``)."""
    return json.dumps(
        {
            "json": {
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "additionalProperties": False,
                "type": "object",
                "required": [],
                "properties": properties,
            },
            "ui": {"fields": {}},
        }
    )


# Run the core type-mapping cases against BOTH envelope shapes so the legacy
# and modern forms get identical coverage. Individual tests below use
# ``_schema_text`` (legacy) for the legacy-specific concerns ({{...}} templates).
_ENVELOPES = pytest.mark.parametrize(
    "wrap", [_schema_text_v1, _schema_text_v2], ids=["legacy", "modern"]
)

# Alias: tests that don't parametrize author against the legacy envelope.
_schema_text = _schema_text_v1


@_ENVELOPES
def test_envelope_scalar_types(wrap):
    struct = json_schema_to_arrow_struct(
        wrap(
            {
                "s": {"type": "string"},
                "n": {"type": "number"},
                "i": {"type": "integer"},
                "b": {"type": "boolean"},
            }
        )
    )
    assert struct == pa.struct(
        [("s", pa.string()), ("n", pa.float64()), ("i", pa.int64()), ("b", pa.bool_())]
    )


@_ENVELOPES
def test_envelope_nested_object(wrap):
    struct = json_schema_to_arrow_struct(
        wrap({"o": {"type": "object", "properties": {"x": {"type": "integer"}}}})
    )
    assert struct == pa.struct([("o", pa.struct([("x", pa.int64())]))])


@_ENVELOPES
def test_envelope_array_of_object(wrap):
    struct = json_schema_to_arrow_struct(
        wrap(
            {
                "items": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {"name": {"type": "string"}},
                    },
                }
            }
        )
    )
    assert struct == pa.struct(
        [("items", pa.list_(pa.struct([("name", pa.string())])))]
    )


@_ENVELOPES
def test_envelope_enum_and_missing_type_default_to_string(wrap):
    struct = json_schema_to_arrow_struct(
        wrap(
            {
                "choice": {"type": "string", "enum": ["a", "b"]},
                "untyped": {"title": "no type here"},
            }
        )
    )
    assert struct == pa.struct([("choice", pa.string()), ("untyped", pa.string())])


@_ENVELOPES
def test_envelope_empty_properties(wrap):
    assert json_schema_to_arrow_struct(wrap({})) == pa.struct([])


# ---------------------------------------------------------------------------
# json_schema_to_arrow_struct
# ---------------------------------------------------------------------------


def test_scalar_types():
    text = _schema_text(
        {
            "s": {"type": "string"},
            "n": {"type": "number"},
            "i": {"type": "integer"},
            "b": {"type": "boolean"},
        }
    )
    struct = json_schema_to_arrow_struct(text)
    assert struct == pa.struct(
        [
            ("s", pa.string()),
            ("n", pa.float64()),
            ("i", pa.int64()),
            ("b", pa.bool_()),
        ]
    )


def test_field_order_preserved():
    text = _schema_text(
        {
            "zebra": {"type": "string"},
            "apple": {"type": "integer"},
            "mango": {"type": "boolean"},
        }
    )
    struct = json_schema_to_arrow_struct(text)
    assert [f.name for f in struct] == ["zebra", "apple", "mango"]


def test_nested_object():
    text = _schema_text(
        {
            "owner": {
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "age": {"type": "integer"},
                },
            }
        }
    )
    struct = json_schema_to_arrow_struct(text)
    assert struct == pa.struct(
        [("owner", pa.struct([("name", pa.string()), ("age", pa.int64())]))]
    )


def test_array_of_scalar():
    text = _schema_text({"tags": {"type": "array", "items": {"type": "string"}}})
    struct = json_schema_to_arrow_struct(text)
    assert struct == pa.struct([("tags", pa.list_(pa.string()))])


def test_array_of_object():
    text = _schema_text(
        {
            "people": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {"name": {"type": "string"}},
                },
            }
        }
    )
    struct = json_schema_to_arrow_struct(text)
    assert struct == pa.struct(
        [("people", pa.list_(pa.struct([("name", pa.string())])))]
    )


def test_array_missing_items_defaults_to_string():
    text = _schema_text({"tags": {"type": "array"}})
    struct = json_schema_to_arrow_struct(text)
    assert struct == pa.struct([("tags", pa.list_(pa.string()))])


def test_object_missing_properties_defaults_to_empty_struct():
    text = _schema_text({"blob": {"type": "object"}})
    struct = json_schema_to_arrow_struct(text)
    assert struct == pa.struct([("blob", pa.struct([]))])


def test_enum_maps_to_string_regardless_of_type():
    text = _schema_text(
        {
            "status": {"type": "integer", "enum": [1, 2, 3]},
            "color": {"enum": ["red", "green"]},
        }
    )
    struct = json_schema_to_arrow_struct(text)
    assert struct == pa.struct([("status", pa.string()), ("color", pa.string())])


def test_missing_type_defaults_to_string():
    text = _schema_text({"mystery": {"title": "Mystery"}})
    struct = json_schema_to_arrow_struct(text)
    assert struct == pa.struct([("mystery", pa.string())])


def test_unknown_type_defaults_to_string():
    text = _schema_text({"weird": {"type": "complex128"}})
    struct = json_schema_to_arrow_struct(text)
    assert struct == pa.struct([("weird", pa.string())])


def test_union_type_as_list_defaults_to_string():
    text = _schema_text({"u": {"type": ["string", "null"]}})
    struct = json_schema_to_arrow_struct(text)
    assert struct == pa.struct([("u", pa.string())])


def test_oneof_anyof_without_type_defaults_to_string():
    text = _schema_text(
        {
            "a": {"oneOf": [{"type": "string"}, {"type": "integer"}]},
            "b": {"anyOf": [{"type": "string"}, {"type": "number"}]},
        }
    )
    struct = json_schema_to_arrow_struct(text)
    assert struct == pa.struct([("a", pa.string()), ("b", pa.string())])


def test_empty_input_returns_empty_struct():
    assert json_schema_to_arrow_struct(None) == pa.struct([])
    assert json_schema_to_arrow_struct("") == pa.struct([])


def test_missing_properties_returns_empty_struct():
    assert json_schema_to_arrow_struct(json.dumps({"schema": {}})) == pa.struct([])
    assert json_schema_to_arrow_struct(json.dumps({})) == pa.struct([])


def test_v2_form_schema_under_json_key():
    """Modern event forms nest the JSON-Schema under ``json`` (draft 2020-12)."""
    text = json.dumps(
        {
            "json": {
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "additionalProperties": False,
                "type": "object",
                "required": [],
                "properties": {
                    "activityrep_whatseen": {"type": "string"},
                    "activityrep_bearing": {"type": "number"},
                    "allfieldscheckboxes": {
                        "type": "array",
                        "items": {
                            "type": "string",
                            "anyOf": [{"$ref": "/api/v2.0/schemas/choices.json"}],
                        },
                    },
                    # choice field: type=string with anyOf $ref -> string
                    "allfieldschoices": {
                        "type": "string",
                        "anyOf": [{"$ref": "/api/v2.0/schemas/choices.json"}],
                    },
                },
            },
            "ui": {"fields": {}},
        }
    )
    struct = json_schema_to_arrow_struct(text)
    assert struct == pa.struct(
        [
            ("activityrep_whatseen", pa.string()),
            ("activityrep_bearing", pa.float64()),
            ("allfieldscheckboxes", pa.list_(pa.string())),
            ("allfieldschoices", pa.string()),
        ]
    )


def test_v2_form_takes_precedence_over_empty_schema_key():
    """When both ``json`` and a fieldless ``schema`` exist, ``json`` wins."""
    text = json.dumps(
        {
            "json": {"type": "object", "properties": {"a": {"type": "string"}}},
            "schema": {"type": "object", "properties": {}},
        }
    )
    assert json_schema_to_arrow_struct(text) == pa.struct([("a", pa.string())])


def test_bare_top_level_properties_fallback():
    text = json.dumps({"type": "object", "properties": {"n": {"type": "number"}}})
    assert json_schema_to_arrow_struct(text) == pa.struct([("n", pa.float64())])


def test_malformed_json_returns_empty_struct():
    assert json_schema_to_arrow_struct("{not json") == pa.struct([])


def test_non_object_json_returns_empty_struct():
    assert json_schema_to_arrow_struct("[1, 2, 3]") == pa.struct([])
    assert json_schema_to_arrow_struct("42") == pa.struct([])


def test_depth_bound_treats_deep_nesting_as_string():
    # Build an object nested far deeper than the depth bound.
    node: dict = {"type": "string"}
    for _ in range(20):
        node = {"type": "object", "properties": {"child": node}}
    text = _schema_text({"root": node})
    # Must not raise and must produce a struct (deep levels degrade to string).
    struct = json_schema_to_arrow_struct(text)
    assert isinstance(struct, pa.StructType)
    assert struct.num_fields == 1
    assert struct.field(0).name == "root"


def test_field_count_bound_caps_total_fields():
    properties = {f"f{i}": {"type": "string"} for i in range(5000)}
    text = _schema_text(properties)
    struct = json_schema_to_arrow_struct(text)
    assert struct.num_fields <= 1000


def test_django_template_placeholders_parse_without_execution():
    # Template tags appear in enum/enumNames value positions and would break
    # JSON parsing if not neutralized. They must never be rendered.
    raw = (
        '{"schema": {"type": "object", "properties": {'
        '"region": {"type": "string", "enum": [{{ regions }}], '
        '"enumNames": [{% for r in regions %}"{{ r.name }}"{% endfor %}]}, '
        '"count": {"type": "integer"}}}, "definition": []}'
    )
    struct = json_schema_to_arrow_struct(raw)
    # enum -> string; count remains integer; no exception raised.
    assert struct == pa.struct([("region", pa.string()), ("count", pa.int64())])


def test_django_template_in_non_enum_position_still_parses():
    raw = (
        '{"schema": {"type": "object", "properties": {'
        '"title": {"type": "string", "title": {{ some_var }}}}}, '
        '"definition": []}'
    )
    struct = json_schema_to_arrow_struct(raw)
    assert struct == pa.struct([("title", pa.string())])


# ---------------------------------------------------------------------------
# Realistic schemas mirroring live-verified event types (regression anchors).
# These reproduce the exact structures observed against the dev tenant so the
# legacy and modern conversion paths stay locked. (The intentionally-corrupt
# legacy schema is deliberately excluded.)
# ---------------------------------------------------------------------------


@_ENVELOPES
def test_string_format_keyword_ignored(wrap):
    """A ``format`` (date-time/date/uri/email) on a string still maps to string."""
    struct = json_schema_to_arrow_struct(
        wrap(
            {
                "when": {"type": "string", "format": "date-time"},
                "day": {"type": "string", "format": "date"},
                "link": {"type": "string", "format": "uri"},
                "mail": {"type": "string", "format": "email"},
            }
        )
    )
    assert struct == pa.struct(
        [
            ("when", pa.string()),
            ("day", pa.string()),
            ("link", pa.string()),
            ("mail", pa.string()),
        ]
    )


@_ENVELOPES
def test_typed_field_with_anyof_ref_uses_declared_type(wrap):
    """A field with both ``type`` and a choices ``$ref`` maps by its ``type``.

    Modern choice fields look like ``{"type": "string", "anyOf": [{"$ref": ...}]}``;
    the declared type wins (string), and array-of-choice -> list<string>.
    """
    struct = json_schema_to_arrow_struct(
        wrap(
            {
                "choice": {
                    "type": "string",
                    "anyOf": [{"$ref": "/api/v2.0/schemas/choices.json?field=x"}],
                },
                "checkboxes": {
                    "type": "array",
                    "items": {
                        "type": "string",
                        "anyOf": [{"$ref": "/api/v2.0/schemas/choices.json?field=y"}],
                    },
                    "uniqueItems": True,
                },
            }
        )
    )
    assert struct == pa.struct(
        [("choice", pa.string()), ("checkboxes", pa.list_(pa.string()))]
    )


def test_realistic_v2_all_field_types():
    """Mirror the live ``all_field_types`` form -> 6-field struct."""
    text = _schema_text_v2(
        {
            "allfieldsfreetext": {"type": "string"},
            "allfieldsnumber": {"type": "number", "minimum": 0},
            "allfieldschoices": {
                "type": "string",
                "anyOf": [{"$ref": "/api/v2.0/schemas/choices.json"}],
            },
            "allfieldsdateandtime": {"type": "string", "format": "date-time"},
            "allfieldscheckboxes": {
                "type": "array",
                "items": {
                    "type": "string",
                    "anyOf": [{"$ref": "/api/v2.0/schemas/choices.json"}],
                },
                "uniqueItems": True,
            },
            "allfieldsscrollingtext": {"type": "string"},
        }
    )
    assert json_schema_to_arrow_struct(text) == pa.struct(
        [
            ("allfieldsfreetext", pa.string()),
            ("allfieldsnumber", pa.float64()),
            ("allfieldschoices", pa.string()),
            ("allfieldsdateandtime", pa.string()),
            ("allfieldscheckboxes", pa.list_(pa.string())),
            ("allfieldsscrollingtext", pa.string()),
        ]
    )


def test_realistic_v1_rhino_sighting_with_templates():
    """Mirror the live ``rhino_sighting_rep`` schema (with ``{{...}}`` tags)."""
    raw = (
        '{"schema": {"$schema": "http://json-schema.org/draft-04/schema#", '
        '"type": "object", "properties": {'
        '"rhinosightingrep_Rhino": {"type": "string", "title": "Individual Rhino ID", '
        '"enum": {{query___blackRhinos___values}}, '
        '"enumNames": {{query___blackRhinos___names}}}, '
        '"rhinosightingrep_earnotchcount": {"type": "number", "title": "Ear notch count"}, '
        '"rhinosightingrep_condition": {"type": "string", "title": "Condition", '
        '"enum": {{enum___rhinosightingrep_condition___values}}, '
        '"enumNames": {{enum___rhinosightingrep_condition___names}}}, '
        '"rhinosightingrep_activity": {"type": "string", "title": "Activity", '
        '"enum": {{enum___rhinosightingrep_activity___values}}, '
        '"enumNames": {{enum___rhinosightingrep_activity___names}}}'
        '}}, "definition": []}'
    )
    assert json_schema_to_arrow_struct(raw) == pa.struct(
        [
            ("rhinosightingrep_Rhino", pa.string()),
            ("rhinosightingrep_earnotchcount", pa.float64()),
            ("rhinosightingrep_condition", pa.string()),
            ("rhinosightingrep_activity", pa.string()),
        ]
    )


def test_realistic_v1_required_fields_nested_collection():
    """Mirror the live ``required_fields`` schema: scalars + array-of-object."""
    text = _schema_text_v1(
        {
            "test_one_date": {"type": "string"},
            "test_two_string": {"type": "string"},
            "test_three_number": {"type": "number", "minimum": 0, "maximum": 50},
            "test_four_number": {"type": "number"},
            "test_nine_dropdown_query": {
                "type": "string",
                "enum": ["__ERTEMPLATE__"],
            },
            "testElevenArrayTest": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "test_array_string": {"type": "string"},
                        "test_array_number": {"type": "number", "minimum": 0},
                    },
                },
            },
            "test_fourteen_textarea": {"type": "string"},
        }
    )
    assert json_schema_to_arrow_struct(text) == pa.struct(
        [
            ("test_one_date", pa.string()),
            ("test_two_string", pa.string()),
            ("test_three_number", pa.float64()),
            ("test_four_number", pa.float64()),
            ("test_nine_dropdown_query", pa.string()),
            (
                "testElevenArrayTest",
                pa.list_(
                    pa.struct(
                        [
                            ("test_array_string", pa.string()),
                            ("test_array_number", pa.float64()),
                        ]
                    )
                ),
            ),
            ("test_fourteen_textarea", pa.string()),
        ]
    )


def test_realistic_v1_single_select_static_inline_enum():
    """Mirror the live ``single_select_static_no_required`` schema."""
    text = _schema_text_v1(
        {
            "single_select_choices": {
                "type": "string",
                "title": "I'm a single select choices",
                "enum": ["Option 1", "Option 2", "Option 3"],
            }
        }
    )
    assert json_schema_to_arrow_struct(text) == pa.struct(
        [("single_select_choices", pa.string())]
    )


# ---------------------------------------------------------------------------
# coerce_details_to_struct
# ---------------------------------------------------------------------------


def _flat_struct() -> pa.StructType:
    return pa.struct(
        [("name", pa.string()), ("age", pa.int64()), ("active", pa.bool_())]
    )


def test_coerce_happy_path():
    struct = _flat_struct()
    values = [json.dumps({"name": "a", "age": 30, "active": True})]
    arr = coerce_details_to_struct(values, struct)
    assert arr.type == struct
    assert len(arr) == 1
    assert arr[0].as_py() == {"name": "a", "age": 30, "active": True}


def test_coerce_missing_key_becomes_null():
    struct = _flat_struct()
    arr = coerce_details_to_struct([json.dumps({"name": "a"})], struct)
    assert arr[0].as_py() == {"name": "a", "age": None, "active": None}


def test_coerce_extra_key_dropped():
    struct = _flat_struct()
    arr = coerce_details_to_struct(
        [json.dumps({"name": "a", "age": 1, "active": False, "extra": "x"})], struct
    )
    assert arr[0].as_py() == {"name": "a", "age": 1, "active": False}


def test_coerce_wrong_typed_value_nulls_only_that_field():
    struct = _flat_struct()
    arr = coerce_details_to_struct(
        [json.dumps({"name": "a", "age": "abc", "active": True})], struct
    )
    assert arr[0].as_py() == {"name": "a", "age": None, "active": True}


def test_coerce_malformed_json_becomes_null_struct():
    struct = _flat_struct()
    arr = coerce_details_to_struct(["{not json"], struct)
    assert len(arr) == 1
    assert arr[0].as_py() is None


def test_coerce_none_cell_becomes_null_struct():
    struct = _flat_struct()
    arr = coerce_details_to_struct([None], struct)
    assert arr[0].as_py() is None


def test_coerce_non_dict_json_becomes_null_struct():
    struct = _flat_struct()
    arr = coerce_details_to_struct(["[]", "5", '"hello"'], struct)
    assert [arr[i].as_py() for i in range(3)] == [None, None, None]


def test_coerce_empty_list_returns_empty_array():
    struct = _flat_struct()
    arr = coerce_details_to_struct([], struct)
    assert arr.type == struct
    assert len(arr) == 0


def test_coerce_empty_struct_type():
    struct = pa.struct([])
    arr = coerce_details_to_struct([json.dumps({"x": 1})], struct)
    assert arr.type == struct
    assert len(arr) == 1


def test_coerce_nested_struct_and_list():
    struct = pa.struct(
        [
            ("owner", pa.struct([("name", pa.string()), ("age", pa.int64())])),
            ("tags", pa.list_(pa.string())),
        ]
    )
    values = [
        json.dumps({"owner": {"name": "a", "age": 5}, "tags": ["x", "y"]}),
    ]
    arr = coerce_details_to_struct(values, struct)
    assert arr[0].as_py() == {
        "owner": {"name": "a", "age": 5},
        "tags": ["x", "y"],
    }


def test_coerce_nested_wrong_type_nulls_nested_field():
    struct = pa.struct(
        [("owner", pa.struct([("name", pa.string()), ("age", pa.int64())]))]
    )
    values = [json.dumps({"owner": {"name": "a", "age": "oops"}})]
    arr = coerce_details_to_struct(values, struct)
    assert arr[0].as_py() == {"owner": {"name": "a", "age": None}}


def test_coerce_batch_with_one_dirty_row():
    struct = _flat_struct()
    values = [
        json.dumps({"name": "good1", "age": 1, "active": True}),
        json.dumps({"name": "dirty", "age": "abc", "active": False}),
        json.dumps({"name": "good2", "age": 2, "active": False}),
    ]
    arr = coerce_details_to_struct(values, struct)
    assert len(arr) == 3
    assert arr[0].as_py() == {"name": "good1", "age": 1, "active": True}
    assert arr[1].as_py() == {"name": "dirty", "age": None, "active": False}
    assert arr[2].as_py() == {"name": "good2", "age": 2, "active": False}


def test_coerce_numeric_truncation_fast_path():
    struct = pa.struct([("age", pa.int64())])
    arr = coerce_details_to_struct([json.dumps({"age": 3.9})], struct)
    assert arr[0].as_py() == {"age": 3}


# ---------------------------------------------------------------------------
# build_details_struct (drop vs coerce of non-conforming rows)
# ---------------------------------------------------------------------------


def _mixed_values() -> list[str | None]:
    """Good, type-mismatch, missing-key, extra-key, unparseable, non-dict, null."""
    return [
        json.dumps({"name": "good", "age": 5, "active": True}),  # conforms
        json.dumps({"name": "x", "age": "abc", "active": True}),  # type mismatch
        json.dumps({"name": "miss"}),  # missing keys -> null (conforms)
        json.dumps({"name": "ex", "age": 1, "active": True, "extra": 9}),  # extra ok
        "{not json",  # unparseable
        "[1, 2, 3]",  # non-object
        None,  # absent details (conforms -> null cell)
    ]


def test_build_details_drop_excludes_only_nonconforming():
    struct = _flat_struct()
    arr, keep = build_details_struct(_mixed_values(), struct, drop_invalid=True)
    # type-mismatch, unparseable, non-dict dropped; good/missing/extra/null kept
    assert keep == [True, False, True, True, False, False, True]
    assert len(arr) == 4
    assert arr.to_pylist() == [
        {"name": "good", "age": 5, "active": True},
        {"name": "miss", "age": None, "active": None},
        {"name": "ex", "age": 1, "active": True},
        None,
    ]


def test_build_details_coerce_keeps_all_rows():
    struct = _flat_struct()
    arr, keep = build_details_struct(_mixed_values(), struct, drop_invalid=False)
    assert keep == [True] * 7
    assert len(arr) == 7
    # Non-conforming rows kept with offending fields / whole cell nulled.
    assert arr.to_pylist() == [
        {"name": "good", "age": 5, "active": True},
        {"name": "x", "age": None, "active": True},
        {"name": "miss", "age": None, "active": None},
        {"name": "ex", "age": 1, "active": True},
        None,
        None,
        None,
    ]


def test_build_details_drop_all_conforming_keeps_everything():
    struct = pa.struct([("age", pa.int64())])
    values = [json.dumps({"age": 1}), None, json.dumps({"age": 2})]
    arr, keep = build_details_struct(values, struct, drop_invalid=True)
    assert keep == [True, True, True]
    assert arr.to_pylist() == [{"age": 1}, None, {"age": 2}]


@pytest.mark.parametrize("drop_invalid", [True, False])
def test_build_details_empty_input(drop_invalid):
    struct = _flat_struct()
    arr, keep = build_details_struct([], struct, drop_invalid=drop_invalid)
    assert keep == []
    assert len(arr) == 0
    assert arr.type == struct


# ---------------------------------------------------------------------------
# required-field handling (event_details_schema + drop-mode enforcement)
# ---------------------------------------------------------------------------


def test_event_details_schema_returns_struct_and_required():
    text = _schema_text_v2(
        {
            "name": {"type": "string"},
            "count": {"type": "number"},
            "note": {"type": "string"},
        }
    )
    # modern envelope helper does not set "required"; inject it.
    obj = json.loads(text)
    obj["json"]["required"] = ["name", "count"]
    struct, required = event_details_schema(json.dumps(obj))

    # Struct is unchanged and fully nullable (required never affects the type).
    assert struct == pa.struct(
        [("name", pa.string()), ("count", pa.float64()), ("note", pa.string())]
    )
    assert all(f.nullable for f in struct)
    assert required.required == frozenset({"name", "count"})
    # Back-compat wrapper returns just the struct.
    assert json_schema_to_arrow_struct(json.dumps(obj)) == struct


def _required_schema() -> str:
    """Top-level required [name, count]; nested object + array-of-object required."""
    return json.dumps(
        {
            "json": {
                "type": "object",
                "required": ["name", "count"],
                "properties": {
                    "name": {"type": "string"},
                    "count": {"type": "number"},
                    "note": {"type": "string"},  # optional
                    "loc": {
                        "type": "object",
                        "required": ["lat"],
                        "properties": {
                            "lat": {"type": "number"},
                            "lon": {"type": "number"},
                        },
                    },
                    "items": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "required": ["sku"],
                            "properties": {
                                "sku": {"type": "string"},
                                "qty": {"type": "number"},
                            },
                        },
                    },
                },
            }
        }
    )


def test_drop_enforces_top_level_required():
    struct, required = event_details_schema(_required_schema())
    values = [
        json.dumps({"name": "a", "count": 1}),  # ok
        json.dumps({"count": 1}),  # missing required name
        json.dumps({"name": "b", "count": None}),  # required count is null
        json.dumps({"name": "c", "count": 2, "note": "x"}),  # optional present, ok
    ]
    arr, keep = build_details_struct(
        values, struct, drop_invalid=True, required=required
    )
    assert keep == [True, False, False, True]
    assert len(arr) == 2


def test_drop_enforces_nested_object_required_only_when_present():
    struct, required = event_details_schema(_required_schema())
    values = [
        json.dumps({"name": "a", "count": 1}),  # loc absent (optional) -> ok
        json.dumps({"name": "b", "count": 1, "loc": {"lon": 5}}),  # loc missing lat
        json.dumps({"name": "c", "count": 1, "loc": {"lat": 1.0}}),  # ok
    ]
    _, keep = build_details_struct(values, struct, drop_invalid=True, required=required)
    assert keep == [True, False, True]


def test_drop_enforces_array_item_required():
    struct, required = event_details_schema(_required_schema())
    values = [
        json.dumps({"name": "a", "count": 1, "items": [{"sku": "x"}, {"qty": 2}]}),
        json.dumps({"name": "b", "count": 1, "items": [{"sku": "y", "qty": 1}]}),
        json.dumps({"name": "c", "count": 1, "items": []}),  # empty list -> ok
    ]
    _, keep = build_details_struct(values, struct, drop_invalid=True, required=required)
    assert keep == [False, True, True]


def test_drop_none_details_dropped_when_required_present():
    struct, required = event_details_schema(_required_schema())
    _, keep = build_details_struct([None], struct, drop_invalid=True, required=required)
    assert keep == [False]


def test_drop_none_details_kept_when_no_required():
    # No required fields anywhere -> a null cell is kept as a null struct.
    text = _schema_text_v2({"a": {"type": "string"}})
    struct, required = event_details_schema(text)
    arr, keep = build_details_struct(
        [None, json.dumps({"a": "x"})], struct, drop_invalid=True, required=required
    )
    assert keep == [True, True]
    assert arr.to_pylist() == [None, {"a": "x"}]


def test_coerce_mode_ignores_required():
    struct, required = event_details_schema(_required_schema())
    values = [json.dumps({"count": 1}), None]  # both fail required
    arr, keep = build_details_struct(
        values, struct, drop_invalid=False, required=required
    )
    assert keep == [True, True]  # coerce never drops
    assert arr.to_pylist()[0]["name"] is None  # missing required -> null, kept


def test_drop_without_required_spec_only_filters_type_errors():
    struct, required = event_details_schema(_required_schema())
    values = [json.dumps({"count": 1})]  # missing required name, but types fine
    # required=None -> required not enforced; row kept.
    _, keep = build_details_struct(values, struct, drop_invalid=True, required=None)
    assert keep == [True]


# ===========================================================================
# parse_datetimes opt-in (format date-time / date -> Arrow temporal)
# ===========================================================================


def _dt_schema(properties: dict) -> str:
    return _schema_text_v2(properties)


def test_parse_datetimes_off_keeps_strings():
    """Default (flag off): date-time / date formats stay pa.string()."""
    text = _dt_schema(
        {
            "when": {"type": "string", "format": "date-time"},
            "day": {"type": "string", "format": "date"},
        }
    )
    struct = json_schema_to_arrow_struct(text)
    assert struct == pa.struct([("when", pa.string()), ("day", pa.string())])


def test_parse_datetimes_on_maps_temporal_leaves():
    text = _dt_schema(
        {
            "when": {"type": "string", "format": "date-time"},
            "day": {"type": "string", "format": "date"},
            "note": {"type": "string"},
        }
    )
    struct = json_schema_to_arrow_struct(text, parse_datetimes=True)
    assert struct == pa.struct(
        [
            ("when", pa.timestamp("us", tz="UTC")),
            ("day", pa.date32()),
            ("note", pa.string()),
        ]
    )


def test_parse_datetimes_enum_precedes_format():
    """An enum stays string even with a date-time format and the flag on."""
    text = _dt_schema(
        {"k": {"type": "string", "enum": ["a", "b"], "format": "date-time"}}
    )
    struct = json_schema_to_arrow_struct(text, parse_datetimes=True)
    assert struct == pa.struct([("k", pa.string())])


def test_parse_datetimes_nested_and_array():
    text = _dt_schema(
        {
            "obj": {
                "type": "object",
                "properties": {"at": {"type": "string", "format": "date-time"}},
            },
            "arr": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {"on": {"type": "string", "format": "date"}},
                },
            },
        }
    )
    struct = json_schema_to_arrow_struct(text, parse_datetimes=True)
    assert struct == pa.struct(
        [
            ("obj", pa.struct([("at", pa.timestamp("us", tz="UTC"))])),
            ("arr", pa.list_(pa.struct([("on", pa.date32())]))),
        ]
    )


def test_parse_datetimes_unknown_format_stays_string():
    text = _dt_schema({"x": {"type": "string", "format": "email"}})
    struct = json_schema_to_arrow_struct(text, parse_datetimes=True)
    assert struct == pa.struct([("x", pa.string())])


def _dt_struct():
    return json_schema_to_arrow_struct(
        _dt_schema(
            {
                "when": {"type": "string", "format": "date-time"},
                "day": {"type": "string", "format": "date"},
            }
        ),
        parse_datetimes=True,
    )


def test_datetime_value_coercion_offset_naive_and_date():
    struct = _dt_struct()
    values = [
        json.dumps({"when": "2024-06-15T08:30:00+02:00", "day": "2024-06-15"}),
        json.dumps({"when": "2024-06-15T08:30:00", "day": "2024-06-15"}),  # naive
    ]
    arr, keep = build_details_struct(values, struct, drop_invalid=False)
    assert keep == [True, True]
    rows = arr.to_pylist()
    # offset preserved as the same instant in UTC
    assert rows[0]["when"] == datetime(2024, 6, 15, 6, 30, tzinfo=timezone.utc)
    # naive assumed UTC
    assert rows[1]["when"] == datetime(2024, 6, 15, 8, 30, tzinfo=timezone.utc)
    assert rows[0]["day"] == date(2024, 6, 15)


def test_datetime_drop_mode_fast_path_keeps_all_valid():
    """All-valid temporal batch in drop mode keeps every row (fast path) and the
    parsed instants survive."""
    struct = _dt_struct()
    values = [
        json.dumps({"when": "2024-06-15T08:30:00Z", "day": "2024-06-15"}),
        json.dumps({"when": "2024-07-01T00:00:00Z", "day": "2024-07-01"}),
    ]
    arr, keep = build_details_struct(values, struct, drop_invalid=True)
    assert keep == [True, True]
    rows = arr.to_pylist()
    assert rows[0]["when"] == datetime(2024, 6, 15, 8, 30, tzinfo=timezone.utc)
    assert rows[1]["day"] == date(2024, 7, 1)


def test_datetime_unparseable_value_coerce_nulls_drop_drops():
    struct = _dt_struct()
    values = [
        json.dumps({"when": "not-a-timestamp", "day": "2024-06-15"}),
        json.dumps({"when": "2024-06-15T00:00:00Z", "day": "2024-06-15"}),
    ]
    # coerce: bad datetime -> null that field, row kept
    arr, keep = build_details_struct(values, struct, drop_invalid=False)
    assert keep == [True, True]
    assert arr.to_pylist()[0]["when"] is None
    # drop: bad datetime is a type mismatch -> row dropped
    _, keep_drop = build_details_struct(values, struct, drop_invalid=True)
    assert keep_drop == [False, True]


# ===========================================================================
# details_invalid_mask
# ===========================================================================


def _required_schema_with_types() -> str:
    return _schema_text_v2(
        {
            "name": {"type": "string"},
            "count": {"type": "integer"},
        }
    )


def test_invalid_mask_agrees_with_drop_keep_mask_when_enforce_required():
    """details_invalid_mask(enforce_required=True) == NOT build_details_struct
    drop keep_mask, across conforming / type-mismatch / missing-required /
    unparseable rows."""
    text = json.dumps(
        {
            "schema": {
                "type": "object",
                "required": ["name"],
                "properties": {
                    "name": {"type": "string"},
                    "count": {"type": "integer"},
                },
            }
        }
    )
    struct, required = event_details_schema(text)
    values = [
        json.dumps({"name": "ok", "count": 1}),  # conforming
        json.dumps({"name": "bad", "count": "NaN"}),  # type mismatch -> invalid
        json.dumps({"count": 2}),  # missing required name -> invalid
        "not json",  # unparseable -> invalid
        json.dumps([1, 2, 3]),  # non-object -> invalid
        None,  # None cell + top-level required -> invalid
    ]
    _, keep = build_details_struct(values, struct, drop_invalid=True, required=required)
    mask = details_invalid_mask(
        values, struct, required=required, enforce_required=True
    )
    assert mask == [not k for k in keep]
    assert mask == [False, True, True, True, True, True]


def test_invalid_mask_coerce_mode_ignores_required():
    """enforce_required=False: a missing optional/required key is NOT invalid,
    but a type mismatch IS."""
    struct, required = event_details_schema(_required_schema_with_types())
    values = [
        json.dumps({"name": "ok", "count": 1}),  # conforming
        json.dumps({"count": 5}),  # missing 'name' -> NOT invalid in coerce mode
        json.dumps({"name": "x", "count": "oops"}),  # type mismatch -> invalid
        None,  # None cell -> not invalid (no required enforced)
    ]
    mask = details_invalid_mask(values, struct, enforce_required=False)
    assert mask == [False, False, True, False]


def test_invalid_mask_with_datetime_leaves():
    """An unparseable temporal value makes a row invalid under both predicates."""
    struct = _dt_struct()
    values = [
        json.dumps({"when": "2024-06-15T00:00:00Z", "day": "2024-06-15"}),
        json.dumps({"when": "nope", "day": "2024-06-15"}),
    ]
    assert details_invalid_mask(values, struct, enforce_required=False) == [
        False,
        True,
    ]
