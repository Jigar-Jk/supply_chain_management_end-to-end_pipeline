# Mock carrier tracking API

A stand-in for a carrier's shipment tracking API (DHL, FedEx, ...), which needs a commercial account.
It serves the shipments written by `data_generators/generate_shipments.py` the way a carrier's REST
API would: API key, incremental `updated_since`, cursor pagination, and the occasional 429 and 500.

The pipeline's API client talks to it exactly as it would to a real carrier, so switching to a real
one later changes only the base URL and the key. The service is not part of the `scm_platform`
wheel and is never deployed to Databricks.

## Run it

```powershell
pip install -r requirements-dev.txt
python -m data_generators.generate_shipments      # if data/shipments/shipments.jsonl is missing
$env:CARRIER_API_KEY = "local-dev-key"
uvicorn services.carrier_api.app:create_app --factory --port 8080
```

- <http://localhost:8080/health> returns `{"status": "ok", "shipments": <count>}`.
- <http://localhost:8080/docs> is the Swagger UI. Click **Authorize**, enter the key, then use
  **Try it out** on `/v1/shipments`.

The data is read once when the app starts, so restart it after regenerating the shipments.

## On Azure

`infra/carrier_api.tf` runs it in Azure Container Apps (HTTPS, one replica). `scripts/up.ps1` builds the
image from `Dockerfile` right after generating the day's data, pushes it to the registry and rolls it
out with `az containerapp update`; the daily pipeline's `fetch_carrier_api` then calls it. Docker is
needed only for that build (a few minutes); without it, the session runs with the API off.

```powershell
docker build -f services/carrier_api/Dockerfile -t carrier-api:local .
docker run --rm -p 8080:80 -e CARRIER_API_KEY=local-dev-key carrier-api:local
```

## API

`GET /v1/shipments` with the header `X-API-Key: <key>`

| Parameter | Meaning |
|---|---|
| `updated_since` | Only shipments changed after this time (ISO 8601 with a time zone, e.g. `2026-11-01T00:00:00Z`). Omit it for everything |
| `limit` | Page size, 1-1000 (default 1000) |
| `cursor` | The `next_cursor` of the previous page, with the same `updated_since` |

```json
{
  "data": [{"shipment_id": "SHP-...", "updated_at": "2026-11-01T08:15:00Z", "...": "..."}],
  "count": 1000,
  "next_cursor": "eyJzaW5jZSI6IC4uLn0="
}
```

Shipments come oldest change first (`updated_at`, then `shipment_id`). `next_cursor` is `null` on
the last page.

| Status | When | What a client should do |
|---|---|---|
| 200 | A page of shipments | Store it and ask for `next_cursor` |
| 400 | Bad `updated_since`, `limit` or `cursor` | Fix the request; retrying does not help |
| 401 | Missing or wrong key | Fix the key; retrying does not help |
| 429 | Rate limited (`CHAOS_429_RATE`) | Wait for the `Retry-After` header's seconds, then retry |
| 500 | Temporary server error (`CHAOS_500_RATE`) | Retry with backoff |

Errors return `{"error": "<code>", "detail": "<text>"}`.

## Settings

| Variable | Default | Meaning |
|---|---|---|
| `CARRIER_API_KEY` | none (required) | The key clients must send |
| `CARRIER_DATA_FILE` | `data/shipments/shipments.jsonl` | Shipments to serve |
| `CHAOS_429_RATE` | `0.05` | Share of requests answered with 429 |
| `CHAOS_500_RATE` | `0.03` | Share of requests answered with 500 |
| `RETRY_AFTER_SECONDS` | `2` | `Retry-After` value on a 429 |

Set both chaos rates to `0` for a well-behaved API.
