import pytest
from pydantic import ValidationError

from ecoscope_earthranger_io_core.query import ObservationsQuery


def test_observations(): ...


class TestObservationsQueryExclusionFlags:
    """Verify the ``exclusion_flags`` field on the shared query model."""

    @pytest.mark.parametrize("value", [None, 0, 1, 2, 3])
    def test_round_trip(self, value):
        q = ObservationsQuery(tenant_domain="example.pamdas.org", exclusion_flags=value)
        assert q.exclusion_flags == value
        assert q.model_dump()["exclusion_flags"] == value

    def test_default_is_none(self):
        q = ObservationsQuery(tenant_domain="example.pamdas.org")
        assert q.exclusion_flags is None

    @pytest.mark.parametrize("value", [None, 0, 1, 2, 3])
    def test_from_query_params_round_trip(self, value):
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
            include_patrol_details=False,
            exclusion_flags=value,
        )
        assert q.exclusion_flags == value

    def test_negative_value_rejected(self):
        with pytest.raises(ValidationError):
            ObservationsQuery(tenant_domain="example.pamdas.org", exclusion_flags=-1)
