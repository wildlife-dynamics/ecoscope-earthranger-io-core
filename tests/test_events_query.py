from datetime import datetime

import pytest
from pydantic import ValidationError

from ecoscope_earthranger_io_core.query import (
    EventsQuery,
    EventTypeSchemaQuery,
    PatrolsQuery,
)


# ---------------------------------------------------------------------------
# EventsQuery — the new shape (breaking change: the old required event_ids is gone)
# ---------------------------------------------------------------------------


def test_events_query_minimal_no_event_ids_required():
    """EventsQuery constructs with only tenant_domain; event_ids no longer exists."""
    q = EventsQuery(tenant_domain="example.pamdas.org")
    assert q.event_type is None
    assert q.state is None
    assert q.include_null_geometry is True
    assert q.range_start is None and q.range_end is None
    assert "event_ids" not in q.model_dump()


def test_events_query_full():
    q = EventsQuery(
        tenant_domain="example.pamdas.org",
        range_start=datetime(2023, 1, 1),
        range_end=datetime(2023, 12, 31),
        event_type=["wildlife_sighting", "poaching"],
        state=["active", "new"],
        include_null_geometry=False,
    )
    assert q.event_type == ["wildlife_sighting", "poaching"]
    assert q.state == ["active", "new"]
    assert q.include_null_geometry is False


def test_events_query_rejects_invalid_state():
    """state is constrained to the EventState literal set."""
    with pytest.raises(ValidationError):
        EventsQuery(tenant_domain="example.pamdas.org", state=["bogus"])


@pytest.mark.parametrize("state", ["active", "new", "resolved", "review"])
def test_events_query_state_unset_dropped_from_wire(state):
    """A set state serializes; an unset (None) state is omitted from the POST body
    via exclude_none, so None means 'no state filter'."""
    q = EventsQuery(tenant_domain="example.pamdas.org", state=[state])
    body = q.model_dump(mode="json", exclude_none=True)
    assert body["state"] == [state]
    assert "state" not in EventsQuery(tenant_domain="example.pamdas.org").model_dump(
        mode="json", exclude_none=True
    )


def test_events_query_rejects_event_ids_kwarg():
    """The removed field must not silently accept the old kwarg (extra forbidden
    only if configured); at minimum it is not a model field."""
    assert "event_ids" not in EventsQuery.model_fields


def test_events_query_detail_shaping_defaults():
    """The detail-shaping options are fields on the shared EventsQuery (single
    source of truth), defaulting to the no-details / typed-drop contract."""
    q = EventsQuery(tenant_domain="example.pamdas.org")
    assert q.include_details is False
    assert q.raw_details is False
    assert q.parse_detail_datetimes is False
    assert q.invalid_details == "drop"
    assert q.invalid_only is False


@pytest.mark.parametrize("include_null_geometry", [True, False])
def test_events_query_from_query_params_round_trip(include_null_geometry):
    q = EventsQuery.from_query_params(
        tenant_domain="example.pamdas.org",
        range_start=None,
        range_end=None,
        event_type=["a", "b"],
        state=["active", "resolved"],
        include_null_geometry=include_null_geometry,
        include_details=True,
        raw_details=False,
        parse_detail_datetimes=True,
        invalid_details="coerce",
        invalid_only=True,
    )
    assert q.event_type == ["a", "b"]
    assert q.state == ["active", "resolved"]
    assert q.include_null_geometry is include_null_geometry
    assert q.include_details is True
    assert q.raw_details is False
    assert q.parse_detail_datetimes is True
    assert q.invalid_details == "coerce"
    assert q.invalid_only is True


# ---------------------------------------------------------------------------
# EventTypeSchemaQuery
# ---------------------------------------------------------------------------


def test_event_type_schema_query():
    q = EventTypeSchemaQuery(
        tenant_domain="example.pamdas.org", event_type="wildlife_sighting"
    )
    assert q.tenant_domain == "example.pamdas.org"
    assert q.event_type == "wildlife_sighting"


def test_event_type_schema_query_requires_event_type():
    with pytest.raises(ValidationError):
        EventTypeSchemaQuery(tenant_domain="example.pamdas.org")


def test_event_type_schema_query_from_query_params():
    q = EventTypeSchemaQuery.from_query_params(
        tenant_domain="example.pamdas.org", event_type="x"
    )
    assert q.event_type == "x"


# ---------------------------------------------------------------------------
# PatrolsQuery.include_events
# ---------------------------------------------------------------------------


def test_patrols_query_include_events_default_true():
    # Default True for drop-in parity with EarthRangerIO.get_patrols, which
    # returns events nested in patrol_segments by default (the workflow chain
    # get_patrols -> unpack_events_from_patrols_df relies on it).
    q = PatrolsQuery(tenant_domain="example.pamdas.org")
    assert q.include_events is True


@pytest.mark.parametrize("include_events", [True, False])
def test_patrols_query_from_query_params_include_events(include_events):
    q = PatrolsQuery.from_query_params(
        tenant_domain="example.pamdas.org",
        range_start=None,
        range_end=None,
        patrol_ids=None,
        patrol_type_value=None,
        patrol_status=None,
        patrols_overlap_daterange=True,
        include_patrol_segments=False,
        include_events=include_events,
        flat=True,
        include_pauses=False,
        raw_details=False,
        parse_detail_datetimes=False,
    )
    assert q.include_events is include_events
