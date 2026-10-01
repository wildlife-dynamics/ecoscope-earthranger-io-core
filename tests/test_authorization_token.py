import asyncio
import base64
import copy
import functools
import json
import logging
import pickle
import time
import types
from collections.abc import Callable, Iterator
from datetime import datetime, timezone
from unittest.mock import patch

import httpx
import pyarrow as pa
import pytest
from pydantic import SecretStr

from ecoscope_earthranger_io_core import client as client_module
from ecoscope_earthranger_io_core.client import (
    _AUTHZ_DEFAULT_TTL_S,
    _AUTHZ_UNSUPPORTED_RETRY_S,
    AUTHORIZATION_TOKEN_HEADER,
    AuthorizationTokenError,
    ERWarehouseClient,
    _parse_expires_at,
)
from ecoscope_earthranger_io_core.query import (
    EventTypeSchemaQuery,
    EventTypesQuery,
    ObservationsQuery,
)

ER_HOST = "some-site.pamdas.org"
DWH_BASE_URL = "https://warehouse.test"
ER_API_TOKEN = "er-api-token-secret"
AUTHZ_PATH = "/api/v1.0/authz/token"


def _jwt(claims: dict) -> str:
    def encode(part: dict) -> str:
        return base64.urlsafe_b64encode(json.dumps(part).encode()).decode().rstrip("=")

    return f"{encode({'alg': 'ES256'})}.{encode(claims)}.signature"


def _iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _arrow_stream() -> bytes:
    table = pa.table({"value": [1, 2, 3]})
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue().to_pybytes()


class FakeServers:
    """ER authz endpoint and DWH API behind one httpx.MockTransport."""

    def __init__(self) -> None:
        self.issued: list[str] = []
        self.er_requests: list[httpx.Request] = []
        self.dwh_requests: list[httpx.Request] = []
        self.er_response: Callable[[int], httpx.Response] = self._issue_token
        self.dwh_statuses: list[int] = []
        self.token_lifetime = 600.0
        self.er_delay = 0.0
        self.now: Callable[[], float] = time.time

    def _issue_token(self, n: int) -> httpx.Response:
        expiry = self.now() + self.token_lifetime
        token = _jwt({"exp": expiry, "n": n})
        self.issued.append(token)
        return httpx.Response(
            200,
            json={"token": token, "expires_at": _iso(expiry), "token_type": "Bearer"},
        )

    def issue_token_ok(self) -> None:
        self.er_response = self._issue_token

    async def handler(self, request: httpx.Request) -> httpx.Response:
        if request.url.host == ER_HOST:
            self.er_requests.append(request)
            if self.er_delay:
                await asyncio.sleep(self.er_delay)
            return self.er_response(len(self.er_requests))
        self.dwh_requests.append(request)
        status = self.dwh_statuses.pop(0) if self.dwh_statuses else 200
        if status != 200:
            return httpx.Response(status, json={"detail": "nope"})
        if request.url.path.endswith("/schema"):
            return httpx.Response(200, json={"field": "string"})
        return httpx.Response(200, content=_arrow_stream())


@pytest.fixture
def servers() -> Iterator[FakeServers]:
    fake = FakeServers()
    transport = httpx.MockTransport(fake.handler)
    with (
        patch.object(
            ERWarehouseClient,
            "_get_id_token",
            return_value=SecretStr("mock-id-token"),
        ),
        patch(
            "httpx.AsyncClient",
            functools.partial(httpx.AsyncClient, transport=transport),
        ),
    ):
        yield fake


class Clock:
    def __init__(self) -> None:
        self.now = float(int(time.time()))

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


@pytest.fixture
def clock(servers: FakeServers, monkeypatch: pytest.MonkeyPatch) -> Clock:
    """Controls the time seen by the client and the fake ER server."""
    fake = Clock()
    monkeypatch.setattr(client_module, "time", types.SimpleNamespace(time=fake))
    servers.now = fake
    return fake


def _client(**kwargs) -> ERWarehouseClient:
    return ERWarehouseClient(
        server=ER_HOST,
        token=SecretStr(ER_API_TOKEN),
        warehouse_base_url=DWH_BASE_URL,
        **kwargs,
    )


def _observations_query() -> ObservationsQuery:
    return ObservationsQuery(
        tenant_domain=ER_HOST,
        subject_ids=["subject1"],
        range_start=datetime(2024, 1, 1),
        range_end=datetime(2024, 1, 2),
    )


class TestParseExpiresAt:
    def test_parses_trailing_z_as_utc(self) -> None:
        expected = datetime(2026, 9, 30, 12, 10, tzinfo=timezone.utc).timestamp()
        assert _parse_expires_at("2026-09-30T12:10:00Z") == expected

    def test_parses_explicit_offset(self) -> None:
        expected = datetime(2026, 9, 30, 10, 10, tzinfo=timezone.utc).timestamp()
        assert _parse_expires_at("2026-09-30T12:10:00+02:00") == expected

    def test_treats_naive_value_as_utc(self) -> None:
        expected = datetime(2026, 9, 30, 12, 10, tzinfo=timezone.utc).timestamp()
        assert _parse_expires_at("2026-09-30T12:10:00") == expected

    @pytest.mark.parametrize("value", [None, "", "not-a-date", 12345])
    def test_returns_none_for_missing_or_invalid_value(self, value) -> None:
        assert _parse_expires_at(value) is None


class TestTokenFetchAndCache:
    @pytest.mark.asyncio
    async def test_fetches_token_once_and_reuses_it(self, servers: FakeServers) -> None:
        client = _client()

        first = await client._get_auth_headers()
        second = await client._get_auth_headers()

        assert len(servers.er_requests) == 1
        assert first[AUTHORIZATION_TOKEN_HEADER] == servers.issued[0]
        assert second[AUTHORIZATION_TOKEN_HEADER] == servers.issued[0]

    @pytest.mark.asyncio
    async def test_requests_token_with_er_token_and_audience(
        self, servers: FakeServers
    ) -> None:
        client = _client(authorization_audience="urn:earthranger:custom")

        await client._get_auth_headers()

        request = servers.er_requests[0]
        assert request.method == "GET"
        assert request.url.path == AUTHZ_PATH
        assert request.url.params["aud"] == "urn:earthranger:custom"
        assert request.headers["Authorization"] == f"Bearer {ER_API_TOKEN}"

    @pytest.mark.asyncio
    async def test_keeps_legacy_headers_next_to_the_token(
        self, servers: FakeServers
    ) -> None:
        headers = await _client()._get_auth_headers()

        assert headers["X-EarthRanger-API-Token"] == ER_API_TOKEN
        assert headers["Authorization"] == "Bearer mock-id-token"
        assert AUTHORIZATION_TOKEN_HEADER in headers

    @pytest.mark.asyncio
    async def test_refreshes_token_within_60_seconds_of_expiry(
        self, servers: FakeServers, clock: Clock
    ) -> None:
        client = _client()

        first = await client._get_auth_headers()
        clock.advance(servers.token_lifetime - 61)
        still_cached = await client._get_auth_headers()
        clock.advance(2)
        refreshed = await client._get_auth_headers()

        assert len(servers.er_requests) == 2
        assert still_cached == first
        assert refreshed[AUTHORIZATION_TOKEN_HEADER] == servers.issued[1]

    @pytest.mark.asyncio
    async def test_uses_expires_at_over_jwt_exp(
        self, servers: FakeServers, clock: Clock
    ) -> None:
        token = _jwt({"exp": clock.now + 3600})
        servers.er_response = lambda n: httpx.Response(
            200, json={"token": token, "expires_at": _iso(clock.now + 1800)}
        )
        client = _client()

        await client._get_authorization_token()

        assert client._authz_token_refresh_at == clock.now + 1800 - 60

    @pytest.mark.asyncio
    async def test_falls_back_to_jwt_exp_when_expires_at_is_missing(
        self, servers: FakeServers
    ) -> None:
        exp = time.time() + 3600
        token = _jwt({"exp": exp})
        servers.er_response = lambda n: httpx.Response(200, json={"token": token})
        client = _client()

        await client._get_auth_headers()
        await client._get_auth_headers()

        assert client._authz_token_refresh_at == exp - 60
        assert len(servers.er_requests) == 1

    @pytest.mark.asyncio
    async def test_concurrent_calls_share_a_single_fetch(
        self, servers: FakeServers
    ) -> None:
        servers.er_delay = 0.05
        client = _client()

        results = await asyncio.gather(*(client._get_auth_headers() for _ in range(10)))

        assert len(servers.er_requests) == 1
        assert {r[AUTHORIZATION_TOKEN_HEADER] for r in results} == {servers.issued[0]}

    def test_lock_works_across_event_loops(self, servers: FakeServers) -> None:
        servers.er_delay = 0.01
        client = _client()

        async def burst() -> None:
            await asyncio.gather(*(client._get_auth_headers() for _ in range(5)))

        asyncio.run(burst())
        client._invalidate_authorization_token(servers.issued[0])
        asyncio.run(burst())

        assert len(servers.er_requests) == 2


class TestDisabledAndFallback:
    @pytest.mark.asyncio
    async def test_header_absent_and_no_fetch_when_disabled(
        self, servers: FakeServers
    ) -> None:
        headers = await _client(use_authorization_token=False)._get_auth_headers()

        assert AUTHORIZATION_TOKEN_HEADER not in headers
        assert headers["X-EarthRanger-API-Token"] == ER_API_TOKEN
        assert servers.er_requests == []

    @pytest.mark.asyncio
    @pytest.mark.parametrize("status", [404, 503])
    async def test_unsupported_site_warns_once_and_uses_legacy_headers(
        self,
        servers: FakeServers,
        caplog: pytest.LogCaptureFixture,
        status: int,
    ) -> None:
        servers.er_response = lambda n: httpx.Response(status)
        client = _client()

        with caplog.at_level(logging.WARNING):
            first = await client._get_auth_headers()
            second = await client._get_auth_headers()
            await client._fetch_observations_arrow(_observations_query())

        assert AUTHORIZATION_TOKEN_HEADER not in first
        assert AUTHORIZATION_TOKEN_HEADER not in second
        assert AUTHORIZATION_TOKEN_HEADER not in servers.dwh_requests[0].headers
        assert len(servers.er_requests) == 1
        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert len(warnings) == 1
        assert f"HTTP {status}" in warnings[0].getMessage()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("status", [400, 401, 403, 500])
    async def test_other_er_errors_raise(
        self, servers: FakeServers, status: int
    ) -> None:
        servers.er_response = lambda n: httpx.Response(
            status, json={"detail": "go away"}
        )
        client = _client()

        with pytest.raises(AuthorizationTokenError, match=f"HTTP {status}") as info:
            await client._get_auth_headers()

        assert "go away" in str(info.value)
        assert ER_API_TOKEN not in str(info.value)
        assert client._authz_unsupported_until == 0.0

    @pytest.mark.asyncio
    async def test_network_error_raises(self, servers: FakeServers) -> None:
        def fail(n: int) -> httpx.Response:
            raise httpx.ConnectError("connection refused")

        servers.er_response = fail

        with pytest.raises(AuthorizationTokenError, match="ConnectError"):
            await _client()._get_auth_headers()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "payload", [{"expires_at": "2026-09-30T12:10:00Z"}, {"token": ""}, []]
    )
    async def test_malformed_response_raises(
        self, servers: FakeServers, payload
    ) -> None:
        servers.er_response = lambda n: httpx.Response(200, json=payload)

        with pytest.raises(AuthorizationTokenError):
            await _client()._get_auth_headers()


class TestUnsupportedRetryWindow:
    @pytest.mark.asyncio
    async def test_503_probes_again_after_the_retry_window(
        self,
        servers: FakeServers,
        clock: Clock,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        servers.er_response = lambda n: httpx.Response(503)
        client = _client()

        with caplog.at_level(logging.WARNING):
            await client._fetch_observations_arrow(_observations_query())
            clock.advance(_AUTHZ_UNSUPPORTED_RETRY_S - 1)
            await client._fetch_observations_arrow(_observations_query())
            warnings_in_window = len(caplog.records)
            servers.issue_token_ok()
            clock.advance(1)
            await client._fetch_observations_arrow(_observations_query())

        assert len(servers.er_requests) == 2
        assert warnings_in_window == 1
        sent = [r.headers.get(AUTHORIZATION_TOKEN_HEADER) for r in servers.dwh_requests]
        assert sent == [None, None, servers.issued[0]]

    @pytest.mark.asyncio
    async def test_503_after_the_window_warns_again(
        self,
        servers: FakeServers,
        clock: Clock,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        servers.er_response = lambda n: httpx.Response(503)
        client = _client()

        with caplog.at_level(logging.WARNING):
            for _ in range(3):
                await client._get_auth_headers()
                clock.advance(_AUTHZ_UNSUPPORTED_RETRY_S / 2)

        assert len(servers.er_requests) == 2
        assert len(caplog.records) == 2

    @pytest.mark.asyncio
    async def test_404_never_probes_again(
        self, servers: FakeServers, clock: Clock
    ) -> None:
        servers.er_response = lambda n: httpx.Response(404)
        client = _client()

        await client._get_auth_headers()
        servers.issue_token_ok()
        clock.advance(365 * 24 * 3600)
        headers = await client._get_auth_headers()

        assert AUTHORIZATION_TOKEN_HEADER not in headers
        assert len(servers.er_requests) == 1


class TestShortLivedTokens:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "payload",
        [
            pytest.param({"token": "not-a-jwt"}, id="unreadable-exp"),
            pytest.param({"token": _jwt({"sub": "x"})}, id="no-exp-claim"),
            pytest.param(
                {"token": _jwt({"sub": "x"}), "expires_at": "garbage"},
                id="bad-expires-at",
            ),
        ],
    )
    async def test_token_without_expiry_is_cached_for_the_default_ttl(
        self,
        servers: FakeServers,
        clock: Clock,
        caplog: pytest.LogCaptureFixture,
        payload: dict,
    ) -> None:
        servers.er_response = lambda n: httpx.Response(200, json=payload)
        client = _client()

        with caplog.at_level(logging.WARNING):
            first = await client._get_auth_headers()
            clock.advance(_AUTHZ_DEFAULT_TTL_S - 1)
            await client._get_auth_headers()
            fetches_within_ttl = len(servers.er_requests)
            clock.advance(1)
            await client._get_auth_headers()

        assert first[AUTHORIZATION_TOKEN_HEADER] == payload["token"]
        assert fetches_within_ttl == 1
        assert len(servers.er_requests) == 2
        assert "no readable expiry" in caplog.records[0].getMessage()
        assert payload["token"] not in caplog.text

    @pytest.mark.asyncio
    async def test_token_expiring_within_60_seconds_is_cached_for_the_default_ttl(
        self, servers: FakeServers, clock: Clock, caplog: pytest.LogCaptureFixture
    ) -> None:
        servers.token_lifetime = 45.0
        client = _client()

        with caplog.at_level(logging.WARNING):
            await client._get_auth_headers()

        assert client._authz_token_refresh_at == clock.now + _AUTHZ_DEFAULT_TTL_S
        assert "45 seconds left" in caplog.records[0].getMessage()
        assert servers.issued[0] not in caplog.text

    @pytest.mark.asyncio
    async def test_token_is_not_cached_past_its_expiry(
        self, servers: FakeServers, clock: Clock
    ) -> None:
        servers.token_lifetime = 10.0
        client = _client()

        await client._get_auth_headers()
        clock.advance(9)
        await client._get_auth_headers()
        clock.advance(1)
        await client._get_auth_headers()

        assert client._authz_token_refresh_at == clock.now + 10
        assert len(servers.er_requests) == 2

    @pytest.mark.asyncio
    async def test_expired_token_is_used_once_but_not_cached(
        self, servers: FakeServers, clock: Clock
    ) -> None:
        servers.token_lifetime = -5.0
        client = _client()

        first = await client._get_auth_headers()
        await client._get_auth_headers()

        assert first[AUTHORIZATION_TOKEN_HEADER] == servers.issued[0]
        assert client._cached_authz_token is None
        assert len(servers.er_requests) == 2


class TestIdToken:
    @pytest.mark.parametrize("raw", ["not-a-jwt", "a.!!!.c"])
    def test_malformed_gcp_id_token_raises(self, raw: str) -> None:
        with patch("google.oauth2.id_token.fetch_id_token", return_value=raw):
            with pytest.raises((IndexError, ValueError)):
                _client()._get_id_token("warehouse.test")

    def test_gcp_id_token_is_cached_until_5_minutes_before_exp(self) -> None:
        raw = _jwt({"exp": time.time() + 3600})
        with patch("google.oauth2.id_token.fetch_id_token", return_value=raw) as fetch:
            client = _client()
            client._get_id_token("warehouse.test")
            token = client._get_id_token("warehouse.test")

        assert token.get_secret_value() == raw
        assert fetch.call_count == 1


class TestPickleAndCopy:
    def test_pickles_a_new_client(self, servers: FakeServers) -> None:
        client = _client(authorization_audience="urn:earthranger:custom")

        restored = pickle.loads(pickle.dumps(client))

        assert restored.authorization_audience == "urn:earthranger:custom"
        assert restored.server == ER_HOST

    @pytest.mark.asyncio
    async def test_pickles_after_lock_and_token_exist_and_drops_the_token(
        self, servers: FakeServers
    ) -> None:
        client = _client()
        await client._get_auth_headers()

        restored = pickle.loads(pickle.dumps(client))

        assert client._cached_authz_token is not None
        assert restored._cached_authz_token is None
        assert restored._authz_token_refresh_at == 0.0
        assert servers.issued[0] not in str(pickle.dumps(client))

    @pytest.mark.asyncio
    async def test_unpickled_client_fetches_a_token_under_a_working_lock(
        self, servers: FakeServers
    ) -> None:
        client = _client()
        await client._get_auth_headers()
        restored = pickle.loads(pickle.dumps(client))
        servers.er_delay = 0.02

        results = await asyncio.gather(
            *(restored._get_auth_headers() for _ in range(5))
        )
        await restored._fetch_observations_arrow(_observations_query())

        assert len(servers.er_requests) == 2
        assert {r[AUTHORIZATION_TOKEN_HEADER] for r in results} == {servers.issued[1]}
        assert (
            servers.dwh_requests[0].headers[AUTHORIZATION_TOKEN_HEADER]
            == servers.issued[1]
        )

    @pytest.mark.asyncio
    async def test_unpickled_503_window_is_kept(self, servers: FakeServers) -> None:
        servers.er_response = lambda n: httpx.Response(503)
        client = _client()
        await client._get_auth_headers()

        restored = pickle.loads(pickle.dumps(client))
        await restored._get_auth_headers()

        assert len(servers.er_requests) == 1

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "copier",
        [
            pytest.param(copy.deepcopy, id="deepcopy"),
            pytest.param(lambda c: c.model_copy(), id="model_copy"),
            pytest.param(lambda c: c.model_copy(deep=True), id="model_copy-deep"),
        ],
    )
    async def test_copies_work_after_lock_and_token_exist(
        self, servers: FakeServers, copier
    ) -> None:
        client = _client()
        await client._get_auth_headers()

        copied = copier(client)
        headers = await copied._get_auth_headers()
        copied._invalidate_authorization_token(servers.issued[0])
        refreshed = await copied._get_auth_headers()

        assert headers[AUTHORIZATION_TOKEN_HEADER] == servers.issued[0]
        assert refreshed[AUTHORIZATION_TOKEN_HEADER] == servers.issued[1]


class TestWarehouse401Retry:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "fetch",
        [
            pytest.param(
                lambda c: c._fetch_observations_arrow(_observations_query()),
                id="post-search-stream",
            ),
            pytest.param(
                lambda c: c._fetch_event_types_arrow(
                    EventTypesQuery(tenant_domain=ER_HOST)
                ),
                id="get-stream",
            ),
            pytest.param(
                lambda c: c._fetch_event_schema(
                    EventTypeSchemaQuery(tenant_domain=ER_HOST, event_type="x"),
                    parse_detail_datetimes=False,
                    fmt="json",
                ),
                id="get-schema",
            ),
        ],
    )
    async def test_refreshes_token_and_retries_once(
        self, servers: FakeServers, fetch
    ) -> None:
        servers.dwh_statuses = [401]
        client = _client()

        result = await fetch(client)

        assert result is not None
        assert len(servers.er_requests) == 2
        assert len(servers.dwh_requests) == 2
        sent = [r.headers[AUTHORIZATION_TOKEN_HEADER] for r in servers.dwh_requests]
        assert sent == servers.issued

    @pytest.mark.asyncio
    async def test_second_401_is_raised(self, servers: FakeServers) -> None:
        servers.dwh_statuses = [401, 401]

        with pytest.raises(httpx.HTTPStatusError) as info:
            await _client()._fetch_observations_arrow(_observations_query())

        assert info.value.response.status_code == 401
        assert len(servers.dwh_requests) == 2
        assert len(servers.er_requests) == 2

    @pytest.mark.asyncio
    async def test_failed_refresh_after_401_chains_the_warehouse_error(
        self, servers: FakeServers
    ) -> None:
        servers.dwh_statuses = [401]
        client = _client()
        await client._get_authorization_token()
        servers.er_response = lambda n: httpx.Response(401, json={"detail": "bad"})

        with pytest.raises(AuthorizationTokenError, match="HTTP 401") as info:
            await client._fetch_observations_arrow(_observations_query())

        cause = info.value.__cause__
        assert isinstance(cause, httpx.HTTPStatusError)
        assert cause.response.status_code == 401
        assert cause.request.url.host == "warehouse.test"
        assert len(servers.dwh_requests) == 1

    @pytest.mark.asyncio
    @pytest.mark.parametrize("status", [403, 500])
    async def test_other_statuses_are_not_retried(
        self, servers: FakeServers, status: int
    ) -> None:
        servers.dwh_statuses = [status]

        with pytest.raises(httpx.HTTPStatusError):
            await _client()._fetch_observations_arrow(_observations_query())

        assert len(servers.dwh_requests) == 1
        assert len(servers.er_requests) == 1

    @pytest.mark.asyncio
    async def test_401_without_authorization_token_is_not_retried(
        self, servers: FakeServers
    ) -> None:
        servers.dwh_statuses = [401]

        with pytest.raises(httpx.HTTPStatusError):
            await _client(use_authorization_token=False)._fetch_observations_arrow(
                _observations_query()
            )

        assert len(servers.dwh_requests) == 1
        assert servers.er_requests == []

    @pytest.mark.asyncio
    async def test_retry_keeps_a_token_already_refreshed_by_another_request(
        self, servers: FakeServers
    ) -> None:
        client = _client()
        await client._get_authorization_token()
        stale = servers.issued[0]
        client._invalidate_authorization_token(stale)
        await client._get_authorization_token()

        client._invalidate_authorization_token(stale)

        assert client._cached_authz_token is not None
        assert client._cached_authz_token.get_secret_value() == servers.issued[1]


class TestTokenValuesAreNotLogged:
    @pytest.mark.asyncio
    async def test_no_token_value_in_logs(
        self, servers: FakeServers, caplog: pytest.LogCaptureFixture
    ) -> None:
        servers.dwh_statuses = [401]
        client = _client()

        with caplog.at_level(logging.DEBUG):
            await client._fetch_observations_arrow(_observations_query())
            servers.er_response = lambda n: httpx.Response(404)
            client._invalidate_authorization_token(servers.issued[-1])
            await client._fetch_observations_arrow(_observations_query())

        assert len(servers.issued) == 2
        assert caplog.records
        for secret in [ER_API_TOKEN, "mock-id-token", *servers.issued]:
            assert secret not in caplog.text
