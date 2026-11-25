import pytest
from pydantic import ValidationError

from ecoscope_earthranger_io_core.query import SubjectSubtypesQuery, SubjectTypesQuery

DOMAIN = "example.pamdas.org"


def test_subject_types_query_defaults():
    q = SubjectTypesQuery(tenant_domain=DOMAIN)
    assert q.tenant_domain == DOMAIN
    assert q.include_inactive is False
    assert q.include_subtypes is False
    # tenant_id is resolved server-side and must never be settable by a caller.
    assert "tenant_id" not in q.model_dump()


def test_subject_types_query_flags_round_trip():
    q = SubjectTypesQuery.model_validate(
        {"include_inactive": True, "include_subtypes": True}
    )
    assert q.include_inactive is True
    assert q.include_subtypes is True


def test_subject_subtypes_query_defaults():
    q = SubjectSubtypesQuery(tenant_domain=DOMAIN)
    assert q.include_inactive is False
    assert q.subject_type is None
    assert "tenant_id" not in q.model_dump()


def test_subject_subtypes_query_type_filter():
    q = SubjectSubtypesQuery(subject_type=["wildlife", "vehicle"])
    assert q.subject_type == ["wildlife", "vehicle"]


def test_subject_subtypes_query_rejects_a_bare_string_type_filter():
    with pytest.raises(ValidationError):
        SubjectSubtypesQuery(subject_type="wildlife")


@pytest.mark.parametrize(
    "model, fields",
    [
        (SubjectTypesQuery, {"include_subtypes": True}),
        (SubjectSubtypesQuery, {"subject_type": ["wildlife"]}),
    ],
)
def test_subject_type_queries_take_no_tenant_in_the_body(model, fields):
    """das sets the tenant itself, so a /search body carries none."""
    assert "tenant_domain" not in model(**fields).model_dump(exclude_none=True)


@pytest.mark.parametrize("model", [SubjectTypesQuery, SubjectSubtypesQuery])
def test_subject_type_queries_are_post_only(model):
    """The listings have no GET route, so there is no query-string binding."""
    assert not hasattr(model, "from_query_params")
