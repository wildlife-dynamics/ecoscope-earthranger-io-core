import asyncio
import json
from collections.abc import Callable
from typing import Any

import httpx
import pyarrow as pa
import pytest

from ecoscope_earthranger_io_core.client import (
    _ERROR_BODY_MAX_CHARS,
    ERWarehouseClient,
)
from ecoscope_earthranger_io_core.query import EventsQuery, PatrolTypeSchemaQuery

DAS_PREFIX = "/api/datasets/v1.0"
# The only query params das forwards to the warehouse; it drops every other one.
DAS_FORWARDED_PARAMS = {
    "store_type",
    "schema",
    "batchsize",
    "format",
    "parse_detail_datetimes",
}
SINCE = "2024-01-01T00:00:00"
UNTIL = "2024-01-31T00:00:00"


def _arrow_stream() -> bytes:
    table = pa.table({"id": pa.array(["a"], type=pa.string())})
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue().to_pybytes()


def _arrow_schema(name: str) -> bytes:
    struct = pa.struct([("weather", pa.string())])
    return pa.schema([(name, struct)]).serialize().to_pybytes()


def _ok_response(request: httpx.Request) -> httpx.Response:
    path = request.url.path
    if request.url.params.get("format") == "json":
        return httpx.Response(200, json={"weather": "string"})
    if path.endswith("/search/segments/schema"):
        return httpx.Response(200, content=_arrow_schema("segment_details"))
    if path.endswith("/patrols/search/schema"):
        return httpx.Response(200, content=_arrow_schema("type_details"))
    if path.endswith("/events/search/schema"):
        return httpx.Response(200, content=_arrow_schema("event_details"))
    return httpx.Response(200, content=_arrow_stream())


class _Recorder:
    def __init__(self) -> None:
        self.requests: list[httpx.Request] = []
        self._respond: Callable[[httpx.Request], httpx.Response] = _ok_response

    def respond_with(self, fn: Callable[[httpx.Request], httpx.Response]) -> None:
        self._respond = fn

    def handle(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        return self._respond(request)


@pytest.fixture
def wire(monkeypatch) -> _Recorder:
    """Route every httpx.AsyncClient the client builds through a mock transport."""
    recorder = _Recorder()
    real_async_client = httpx.AsyncClient

    def _factory(*args: Any, **kwargs: Any) -> httpx.AsyncClient:
        transport = httpx.MockTransport(recorder.handle)
        return real_async_client(*args, transport=transport, **kwargs)

    monkeypatch.setattr(httpx, "AsyncClient", _factory)
    return recorder


def _client(server: str = "site-a.pamdas.org", **kwargs: Any) -> ERWarehouseClient:
    return ERWarehouseClient(server=server, token="er-token", **kwargs)


def _body(request: httpx.Request) -> dict[str, Any]:
    return json.loads(request.content)


# -------------------------------------------------------------------------
# Base URL
# -------------------------------------------------------------------------


@pytest.mark.parametrize("server", ["site-a.pamdas.org", "site-b.pamdas.org"])
def test_requests_go_to_the_das_proxy_of_the_clients_own_site(
    wire, server: str
) -> None:
    _client(server).get_event_types()

    (request,) = wire.requests
    assert request.url.scheme == "https"
    assert request.url.host == server
    assert request.url.path == f"{DAS_PREFIX}/event_types/search"


def test_warehouse_base_url_overrides_the_das_url_as_is(wire) -> None:
    _client(warehouse_base_url="http://localhost:8000/custom").get_event_types()

    (request,) = wire.requests
    assert str(request.url.copy_with(query=None)) == (
        "http://localhost:8000/custom/event_types/search"
    )


# -------------------------------------------------------------------------
# Auth
# -------------------------------------------------------------------------


def test_sends_the_er_token_as_bearer_and_nothing_else(wire) -> None:
    _client().get_subjectgroup_observations("Elephants", since=SINCE, until=UNTIL)

    (request,) = wire.requests
    assert request.headers["Authorization"] == "Bearer er-token"
    assert "X-EarthRanger-API-Token" not in request.headers


def test_makes_no_status_call(wire) -> None:
    client = _client()
    client.get_event_types()
    client.get_patrol_types()

    assert len(wire.requests) == 2
    for request in wire.requests:
        assert request.url.path.startswith(f"{DAS_PREFIX}/")
        assert "status" not in request.url.path


# -------------------------------------------------------------------------
# The eight das routes
# -------------------------------------------------------------------------

_ROUTE_CALLS: dict[str, tuple[Callable[[ERWarehouseClient], Any], dict[str, Any]]] = {
    "observations/search/stream/arrow": (
        lambda c: c.get_subjectgroup_observations(
            "Elephants", since=SINCE, until=UNTIL
        ),
        {"subject_group_name": "Elephants", "range_start": SINCE},
    ),
    "patrols/search/stream/arrow": (
        lambda c: c.get_patrols_minimal(
            since=SINCE, until=UNTIL, patrol_type_value=["routine_patrol"]
        ),
        {"patrol_type_value": ["routine_patrol"], "include_events": False},
    ),
    "events/search/stream/arrow": (
        lambda c: c.get_events(since=SINCE, until=UNTIL, event_type=["fire"]),
        {"event_type": ["fire"], "range_end": UNTIL},
    ),
    "event_types/search": (lambda c: c.get_event_types(), {}),
    "patrol_types/search": (lambda c: c.get_patrol_types(), {}),
    "events/search/schema": (
        lambda c: c.get_event_schema("fire"),
        {"event_type": "fire"},
    ),
    "patrols/search/schema": (
        lambda c: c.get_patrol_schema("routine_patrol"),
        {"patrol_type_value": "routine_patrol"},
    ),
    "patrols/search/segments/schema": (lambda c: c.get_segment_schema(), {}),
}


@pytest.mark.parametrize("route", list(_ROUTE_CALLS))
def test_each_read_posts_a_json_body_to_its_das_route(wire, route) -> None:
    call, expected_body = _ROUTE_CALLS[route]
    call(_client())

    (request,) = wire.requests
    assert request.method == "POST"
    assert request.url.path == f"{DAS_PREFIX}/{route}"
    assert request.headers["content-type"] == "application/json"
    body = _body(request)
    assert isinstance(body, dict)
    assert body.items() >= expected_body.items()
    assert "tenant_domain" not in body
    assert request.url.params.get("store_type") == "auto"
    assert set(request.url.params) <= DAS_FORWARDED_PARAMS


def test_tenant_domain_set_on_the_query_is_never_sent(wire) -> None:
    client = _client()
    asyncio.run(
        client._fetch_events_arrow(EventsQuery(tenant_domain="x", event_type=["fire"]))
    )
    asyncio.run(
        client._fetch_patrol_schema(
            PatrolTypeSchemaQuery(tenant_domain="x", patrol_type_value="p"),
            parse_detail_datetimes=False,
            fmt="arrow",
        )
    )

    assert len(wire.requests) == 2
    for request in wire.requests:
        body = _body(request)
        assert "tenant_domain" not in body
        assert body


def test_schema_options_travel_in_forwarded_query_params(wire) -> None:
    _client().get_patrol_schema(
        "routine_patrol",
        parse_detail_datetimes=True,
        format="json",
        query_engine="iceberg-dd",
    )

    (request,) = wire.requests
    assert dict(request.url.params) == {
        "store_type": "iceberg-dd",
        "format": "json",
        "parse_detail_datetimes": "true",
    }
    assert _body(request) == {"patrol_type_value": "routine_patrol"}


def test_patrol_ids_travel_in_the_body(wire) -> None:
    patrols = pa.table({"id": pa.array(["p1"], type=pa.string())})
    _client().get_patrol_observations(patrols)

    (request,) = wire.requests
    assert _body(request)["patrol_ids"] == ["p1"]
    assert set(request.url.params) <= DAS_FORWARDED_PARAMS


def test_reads_the_schema_route_responses(wire) -> None:
    schema = _client().get_segment_schema()

    assert schema.field("segment_details").type == pa.struct([("weather", pa.string())])


# -------------------------------------------------------------------------
# Error detail from das and from the warehouse
# -------------------------------------------------------------------------

_DAS_ERROR = {
    "status": {
        "code": 403,
        "message": "Forbidden",
        "detail": "You do not have permission to read this data.",
    }
}


@pytest.mark.parametrize(
    "status, payload, expected",
    [
        (403, _DAS_ERROR, "You do not have permission to read this data."),
        (
            404,
            {"status": {"code": 404, "message": "Not found", "detail": None}},
            "Not found",
        ),
        (422, {"detail": "Subject group 'x' not found."}, "Subject group 'x'"),
        (
            422,
            {"detail": [{"loc": ["body", "range_start"], "msg": "bad date"}]},
            "body.range_start: bad date",
        ),
    ],
)
def test_error_message_shows_the_detail(
    wire, status: int, payload: dict, expected: str
) -> None:
    wire.respond_with(lambda request: httpx.Response(status, json=payload))

    with pytest.raises(httpx.HTTPStatusError) as exc_info:
        _client().get_events(since=SINCE, until=UNTIL)

    assert exc_info.value.response.status_code == status
    assert expected in str(exc_info.value).splitlines()[0]


def test_schema_404_from_the_warehouse_raises_value_error(wire) -> None:
    wire.respond_with(
        lambda request: httpx.Response(
            404, json={"detail": "Patrol type 'x' not found for this tenant."}
        )
    )

    with pytest.raises(ValueError, match="Patrol type 'x' not found"):
        _client().get_patrol_schema("x")


@pytest.mark.parametrize("payload", [{"detail": None}, {"status": "error"}])
def test_error_message_falls_back_to_the_body_text(wire, payload: dict) -> None:
    wire.respond_with(lambda request: httpx.Response(400, json=payload))

    with pytest.raises(httpx.HTTPStatusError) as exc_info:
        _client().get_events(since=SINCE, until=UNTIL)

    response = exc_info.value.response
    assert response.status_code == 400
    assert response.text in str(exc_info.value).splitlines()[0]


def test_5xx_text_body_is_shown_truncated(wire) -> None:
    html = "<html><body>" + "x" * (2 * _ERROR_BODY_MAX_CHARS) + "</body></html>"
    wire.respond_with(
        lambda request: httpx.Response(
            502, text=html, headers={"content-type": "text/html"}
        )
    )

    with pytest.raises(httpx.HTTPStatusError) as exc_info:
        _client().get_events(since=SINCE, until=UNTIL)

    assert exc_info.value.response.status_code == 502
    first_line = str(exc_info.value).splitlines()[0]
    assert html[:_ERROR_BODY_MAX_CHARS] + "..." in first_line
    assert html[: _ERROR_BODY_MAX_CHARS + 1] not in first_line


def test_schema_404_for_a_missing_warehouse_route_raises_http_status_error(
    wire,
) -> None:
    wire.respond_with(lambda request: httpx.Response(404, json={"detail": "Not Found"}))

    with pytest.raises(httpx.HTTPStatusError) as exc_info:
        _client().get_patrol_schema("x")

    assert exc_info.value.response.status_code == 404


@pytest.mark.parametrize(
    "call",
    [
        lambda c: c.get_event_schema("x"),
        lambda c: c.get_segment_schema(),
    ],
    ids=["event_schema", "segment_schema"],
)
def test_schema_404_without_a_not_found_hint_raises_http_status_error(
    wire, call: Callable[[ERWarehouseClient], Any]
) -> None:
    wire.respond_with(
        lambda request: httpx.Response(
            404, json={"detail": "Event type 'x' not found for this tenant."}
        )
    )

    with pytest.raises(httpx.HTTPStatusError) as exc_info:
        call(_client())

    assert exc_info.value.response.status_code == 404
    assert "not found for this tenant" in str(exc_info.value).splitlines()[0]


def test_schema_404_from_das_raises_http_status_error(wire) -> None:
    wire.respond_with(
        lambda request: httpx.Response(
            404,
            json={"status": {"code": 404, "message": "Not found", "detail": None}},
        )
    )

    with pytest.raises(httpx.HTTPStatusError) as exc_info:
        _client().get_patrol_schema("x")

    assert exc_info.value.response.status_code == 404


def test_token_stays_out_of_repr_and_errors(wire) -> None:
    client = _client()
    wire.respond_with(
        lambda request: httpx.Response(403, json={"detail": "Forbidden."})
    )

    with pytest.raises(httpx.HTTPStatusError) as exc_info:
        client.get_events(since=SINCE, until=UNTIL)

    assert "er-token" not in repr(client)
    assert "er-token" not in str(exc_info.value)
