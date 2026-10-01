# ecoscope-earthranger-io-core
Core models for Ecoscope interface with EarthRanger.

## Authentication

`ERWarehouseClient` sends these headers on every warehouse API request:

- `X-EarthRanger-API-Token`: the EarthRanger token passed as `token`.
- `Authorization`: a Google Cloud ID token for the warehouse Cloud Run service,
  from Application Default Credentials.
- `X-EarthRanger-Authorization`: a signed authorization token from EarthRanger.

The client gets the authorization token automatically from
`GET https://{server}/api/v1.0/authz/token?aud={authorization_audience}`, using
the EarthRanger token. It caches the token until 60 seconds before it expires. If
the warehouse rejects a request with 401, the client gets a new token and
retries the request once.

If the EarthRanger site does not issue authorization tokens (HTTP 404), the
client logs one warning and then uses only the first two headers for the lifetime
of the client instance. On HTTP 503 (signing not configured) it does the same for
5 minutes, then asks for a token again. Any other failure (for example 400, 401,
403 or a network error) raises `AuthorizationTokenError`.

An invalid or expired EarthRanger token now fails at the token endpoint, so it
raises `AuthorizationTokenError` instead of the `httpx.HTTPStatusError` (401)
that the warehouse returned before. When the token refresh after a warehouse 401
fails, the warehouse `HTTPStatusError` is available as the exception's
`__cause__`.

Tokens that expire within 60 seconds, or have no readable expiry, are cached for
at most 30 seconds. A pickled client does not keep its authorization token; it
gets a new one when it is used.

Options:

- `use_authorization_token` (default `True`): set to `False` to never request
  the authorization token.
- `authorization_audience` (default `"urn:earthranger:dwh"`): the `aud` value
  requested from EarthRanger.

```python
from pydantic import SecretStr
from ecoscope_earthranger_io_core.client import ERWarehouseClient

client = ERWarehouseClient(server="mep-dev.pamdas.org", token=SecretStr("..."))
```
