import pytest
from pydantic import ValidationError

from ecoscope_earthranger_io_core.query import (
    PatrolsQuery,
    PatrolTypeSchemaQuery,
    PatrolTypesQuery,
    SegmentSchemaQuery,
)


DOMAIN = "example.pamdas.org"

# Every PatrolsQuery field that existed before this change, with its default.
# The new options must not disturb any of them.
PRE_EXISTING_PATROLS_DEFAULTS = {
    "range_start": None,
    "range_end": None,
    "patrol_ids": None,
    "patrol_type_value": None,
    "patrol_status": None,
    "patrols_overlap_daterange": True,
    "include_patrol_segments": False,
    "include_events": True,
    "flat": True,
}


def test_patrols_query_existing_defaults_are_unchanged():
    """A caller that passes none of the new options must behave exactly as
    before -- the new fields are additive inputs, not a behaviour change."""
    q = PatrolsQuery(tenant_domain=DOMAIN)
    for name, default in PRE_EXISTING_PATROLS_DEFAULTS.items():
        assert getattr(q, name) == default


def test_patrols_query_excludes_pauses_by_default():
    """das excludes paused legs by default so patrol totals match the product;
    the warehouse query mirrors that."""
    assert PatrolsQuery(tenant_domain=DOMAIN).include_pauses is False


def test_patrols_query_detail_options_default_to_typed():
    assert PatrolsQuery(tenant_domain=DOMAIN).raw_details is False
    assert PatrolsQuery(tenant_domain=DOMAIN).parse_detail_datetimes is False


@pytest.mark.parametrize(
    "field", ["include_pauses", "raw_details", "parse_detail_datetimes"]
)
@pytest.mark.parametrize("value", [True, False])
def test_patrols_query_new_flags_round_trip(field, value):
    q = PatrolsQuery(**{"tenant_domain": DOMAIN, field: value})
    assert getattr(q, field) is value
    assert q.model_dump()[field] is value


def test_patrols_query_from_query_params_round_trip():
    # ``from_query_params`` is a FastAPI dependency; when called directly we must
    # pass every parameter explicitly so the ``Query(...)`` sentinels don't leak
    # into pydantic.
    q = PatrolsQuery.from_query_params(
        tenant_domain=DOMAIN,
        range_start=None,
        range_end=None,
        patrol_ids=None,
        patrol_type_value=None,
        patrol_status=None,
        patrols_overlap_daterange=True,
        include_patrol_segments=False,
        include_events=True,
        flat=True,
        include_pauses=True,
        raw_details=True,
        parse_detail_datetimes=True,
    )
    assert q.include_pauses is True
    assert q.raw_details is True
    assert q.parse_detail_datetimes is True


def test_patrol_types_query_is_tenant_scoped():
    q = PatrolTypesQuery(tenant_domain=DOMAIN)
    assert q.tenant_domain == DOMAIN
    assert q.include_schema is False
    # tenant_id is resolved server-side and must never be settable by a caller.
    assert "tenant_id" not in q.model_dump()


@pytest.mark.parametrize("value", [True, False])
def test_patrol_types_query_include_schema_round_trip(value):
    q = PatrolTypesQuery.from_query_params(tenant_domain=DOMAIN, include_schema=value)
    assert q.include_schema is value


def test_patrol_type_schema_query_requires_its_key():
    q = PatrolTypeSchemaQuery(tenant_domain=DOMAIN, patrol_type_value="routine_patrol")
    assert q.patrol_type_value == "routine_patrol"
    with pytest.raises(ValidationError):
        PatrolTypeSchemaQuery(tenant_domain=DOMAIN)


def test_patrol_type_schema_query_accepts_a_uuid_for_its_key():
    """das keys a patrol type by either its UUID or its ``value`` slug. One
    field takes both spellings, so a caller cannot set two conflicting keys."""
    uuid = "c4d5e6f7-1a2b-4c3d-8e9f-0a1b2c3d4e5f"
    q = PatrolTypeSchemaQuery.from_query_params(
        tenant_domain=DOMAIN, patrol_type_value=uuid
    )
    assert q.patrol_type_value == uuid


def test_segment_schema_query_is_keyed_only_by_tenant():
    """There is exactly one segment schema per tenant, so tenant is the whole
    key -- and that is why segment_details can always be served typed."""
    q = SegmentSchemaQuery.from_query_params(tenant_domain=DOMAIN)
    assert q.model_dump() == {"tenant_domain": DOMAIN}


@pytest.mark.parametrize(
    "model", [PatrolTypesQuery, PatrolTypeSchemaQuery, SegmentSchemaQuery]
)
def test_schema_queries_require_a_tenant(model):
    with pytest.raises(ValidationError):
        model()
