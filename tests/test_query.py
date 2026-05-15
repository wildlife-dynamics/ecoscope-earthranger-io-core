import pytest
from pydantic import ValidationError

from ecoscope_earthranger_io_core.query import ObservationsQuery, PatrolsQuery


def test_observations(): ...


@pytest.mark.parametrize("value", [None, 0, 1, 2, 3])
def test_observations_query_exclusion_flags_round_trip(value):
    q = ObservationsQuery(tenant_domain="example.pamdas.org", exclusion_flags=value)
    assert q.exclusion_flags == value
    assert q.model_dump()["exclusion_flags"] == value


def test_observations_query_exclusion_flags_default_is_none():
    q = ObservationsQuery(tenant_domain="example.pamdas.org")
    assert q.exclusion_flags is None


@pytest.mark.parametrize("value", [None, 0, 1, 2, 3])
def test_observations_query_from_query_params_round_trip(value):
    # ``from_query_params`` is a FastAPI dependency; when called directly
    # we must pass every parameter explicitly so the ``Query(...)``
    # sentinels don't leak into pydantic.
    q = ObservationsQuery.from_query_params(
        tenant_domain="example.pamdas.org",
        range_start=None,
        range_end=None,
        subject_ids=None,
        subject_group_name=None,
        patrol_ids=None,
        patrol_type_value=None,
        patrol_status=None,
        patrols_overlap_daterange=True,
        include_patrol_details=False,
        exclusion_flags=value,
    )
    assert q.exclusion_flags == value


def test_observations_query_exclusion_flags_negative_value_rejected():
    with pytest.raises(ValidationError):
        ObservationsQuery(tenant_domain="example.pamdas.org", exclusion_flags=-1)


def test_observations_query_patrols_overlap_daterange_default_is_true():
    q = ObservationsQuery(tenant_domain="example.pamdas.org")
    assert q.patrols_overlap_daterange is True


@pytest.mark.parametrize("value", [True, False])
def test_observations_query_patrols_overlap_daterange_round_trip(value):
    q = ObservationsQuery(
        tenant_domain="example.pamdas.org", patrols_overlap_daterange=value
    )
    assert q.patrols_overlap_daterange is value
    assert q.model_dump()["patrols_overlap_daterange"] is value


@pytest.mark.parametrize("value", [True, False])
def test_observations_query_from_query_params_patrols_overlap_daterange(value):
    # ``from_query_params`` is a FastAPI dependency; when called directly
    # we must pass every parameter explicitly so the ``Query(...)``
    # sentinels don't leak into pydantic.
    q = ObservationsQuery.from_query_params(
        tenant_domain="example.pamdas.org",
        range_start=None,
        range_end=None,
        subject_ids=None,
        subject_group_name=None,
        patrol_ids=None,
        patrol_type_value=None,
        patrol_status=None,
        patrols_overlap_daterange=value,
        include_patrol_details=False,
        exclusion_flags=None,
    )
    assert q.patrols_overlap_daterange is value


def test_patrols_query_patrols_overlap_daterange_default_is_true():
    q = PatrolsQuery(tenant_domain="example.pamdas.org")
    assert q.patrols_overlap_daterange is True


@pytest.mark.parametrize("value", [True, False])
def test_patrols_query_patrols_overlap_daterange_round_trip(value):
    q = PatrolsQuery(
        tenant_domain="example.pamdas.org", patrols_overlap_daterange=value
    )
    assert q.patrols_overlap_daterange is value
    assert q.model_dump()["patrols_overlap_daterange"] is value


@pytest.mark.parametrize("value", [True, False])
def test_patrols_query_from_query_params_patrols_overlap_daterange(value):
    # ``from_query_params`` is a FastAPI dependency; when called directly
    # we must pass every parameter explicitly so the ``Query(...)``
    # sentinels don't leak into pydantic.
    q = PatrolsQuery.from_query_params(
        tenant_domain="example.pamdas.org",
        range_start=None,
        range_end=None,
        patrol_ids=None,
        patrol_type_value=None,
        patrol_status=None,
        patrols_overlap_daterange=value,
        include_patrol_segments=False,
        flat=True,
    )
    assert q.patrols_overlap_daterange is value
