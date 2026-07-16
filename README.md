# Trading Memory Engine v1.0

Independent event-sourced trading memory service. It owns raw truth, lifecycle
projections, recovery, snapshots and wallet health. HRS reads only the current
projection and never reconstructs history.

## Guarantees

- `event_journal` is append-only, enforced by database triggers.
- A fill and its projection update commit in one transaction.
- `event_id` makes ingestion idempotent.
- Exactly one materialized row exists per wallet/coin.
- Raw events remain queryable as wallet → coin → lifecycle → fills.
- Recovery loads the latest snapshot, replays only its tail, then reconciles
  against authoritative current state supplied by the gRPC collector.
- `PURGE` means purge derived state. The immutable audit journal is retained by
  design; physical journal destruction is intentionally not exposed.

## Run

No runtime packages are required:

```powershell
python -m tme.cli --db data/tme.db serve --port 8080
```

Open `http://127.0.0.1:8080`. Add a wallet and ingest a fill:

```powershell
python -m tme.cli --db data/tme.db wallet-add 0xabc --label Alpha
python -m tme.cli --db data/tme.db ingest-fill '{"wallet":"0xabc","coin":"BTC","side":"BUY","size":"1.5","price":"65000","timestamp_ms":1784246400000,"event_id":"fill-1"}'
```

## API contract

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/v1/wallets` | Add wallet without restart |
| `POST` | `/v1/wallets/{wallet}/{PAUSE|RESUME|ARCHIVE|REMOVE|PURGE}` | Wallet manager |
| `POST` | `/v1/events/fills` | Collector ingestion boundary |
| `GET` | `/v1/projection/{wallet}` | HRS current-state read |
| `GET` | `/v1/projection/{wallet}/{coin}` | HRS one coin read |
| `GET` | `/v1/raw/{wallet}?coin=BTC&lifecycle=18` | Raw Explorer |
| `POST` | `/v1/snapshots/{wallet}/create` | Create recovery snapshot |
| `POST` | `/v1/recovery/{wallet}/run` | Snapshot + tail + current-state reconciliation |
| `GET` | `/v1/report` | Live wallet and market report |

The QuickNode adapter should do only protobuf-to-canonical mapping and call
`Collector.accept_fill()` / `Collector.accept_state()`. This keeps provider
credentials and generated protobuf modules outside the memory core.

## Test

```powershell
python -m unittest discover -s tests -v
```


## Railway

The repository includes `railway.json` and uses the root `Dockerfile`. Railway's
injected `PORT` is used automatically, with `TME_PORT` as a local fallback. Mount
a persistent Railway volume at `/data` so the SQLite journal survives redeploys.

## HRS Processing and Ready Queue

The `tme serve` process runs a durable HRS-compatible processing worker after each
journal commit. Processing state is isolated by wallet and coin. It ports the
existing HRS lifecycle, capital, velocity, acceleration, eligibility, ranking and
decision rules; HRS consumes only durable `ExecutionIntent` messages.

Endpoints:

- `GET /v1/ready/next?consumer=hrs-executor&wait=25`
- `POST /v1/ready/{message_id}/ack`
- `POST /v1/ready/{message_id}/nack`
- `POST /v1/processing/held`
- `GET /v1/processing/health`

Set `HRS_ALLOCATION_GAP_PCT` on this service to the same strategy value used by
HRS. Wallet ADD/PAUSE/RESUME/REMOVE state is read from the existing SQLite wallet
manager without restarting the worker.
## Hyperliquid Collector

When `HC_GRPC_ENDPOINT` and `HC_GRPC_TOKEN` are configured, `tme serve` starts the
collector automatically. It subscribes before bootstrapping, buffers live fills,
loads regular and TWAP history plus authoritative state, snapshots, reconciles,
and then drains the live buffer. Wallets come only from SQLite Wallet Manager;
ADD/PAUSE/RESUME/REMOVE is detected without restarting the service.