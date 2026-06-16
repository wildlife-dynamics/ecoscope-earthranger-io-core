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
    assert q.include_null_geometry is True
    assert q.range_start is None and q.range_end is None
    assert "event_ids" not in q.model_dump()


def test_events_query_full():
    q = EventsQuery(
        tenant_domain="example.pamdas.org",
        range_start=datetime(2023, 1, 1),
        range_end=datetime(2023, 12, 31),
        event_type=["wildlife_sighting", "poaching"],
        include_null_geometry=False,
    )
    assert q.event_type == ["wildlife_sighting", "poaching"]
    assert q.include_null_geometry is False


def test_events_query_rejects_event_ids_kwarg():
    """The removed field must not silently accept the old kwarg (extra forbidden
    only if configured); at minimum it is not a model field."""
    assert "event_ids" not in EventsQuery.model_fields


@pytest.mark.parametrize("include_null_geometry", [True, False])
def test_events_query_from_query_params_round_trip(include_null_geometry):
    q = EventsQuery.from_query_params(
        tenant_domain="example.pamdas.org",
        range_start=None,
        range_end=None,
        event_type=["a", "b"],
        include_null_geometry=include_null_geometry,
    )
    assert q.event_type == ["a", "b"]
    assert q.include_null_geometry is include_null_geometry


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


def test_patrols_query_include_events_default_false():
    q = PatrolsQuery(tenant_domain="example.pamdas.org")
    assert q.include_events is False


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
    )
    assert q.include_events is include_events
