# Live Backend Evolution

This note captures the first two production-capable versions of the live backend, why the second version exists, and what the production evidence looked like immediately after the decoupled rollout.

## V1: Inline Ingestion On One Box

The initial working version was deliberately simple.

- One `scripts/live_ingester.py` process owned the round-robin polling loop.
- `scripts/live_api.py` exposed both the read API and the push endpoint.
- Push requests were ingested inline inside the HTTP request path.
- Polls were also fetched, decoded, parsed, matched, and written to SQLite inline.
- SQLite in WAL mode was the single live-state store.
- Raw payloads were written to disk and later archived into daily `.tgz` bundles for Hugging Face.

That version was good enough to prove the product and the AFIR live model on a single VPS. It was easy to debug because receipt and ingestion happened in one place.

The main limitation was coupling:

- a slow poll blocked the next provider in the round-robin loop
- a slow push blocked the API request until decode and SQLite writes finished
- heavy providers stretched the whole cycle, even for small providers

This showed up clearly in production on `2026-04-18` and `2026-04-19`: heavy providers reduced overall polling cadence and made the daily archive much smaller because far fewer fetches completed.

## V2: Decoupled Receipt And Ingestion

The second version keeps the single-host model, but separates receipt from ingestion.

- `scripts/live_ingester.py` is now a receipt loop for pull.
- `scripts/live_api.py` receives push traffic and acknowledges quickly.
- `scripts/live_queue_worker.py` drains a durable file-backed receipt queue.
- The queue lives under `WOLADEN_LIVE_QUEUE_DIR` with `pending/`, `processing/`, `done/`, and `failed/`.
- The worker performs decode, normalization, matching, and SQLite writes outside the receipt path.
- Providers can now be `poll_only`, `push_only`, or `push_with_poll_fallback`.

The important design point is that push and pull both become receipt events first. We already keep the raw files anyway for archives, so the queue formalizes that boundary instead of doing expensive parsing on the critical path.

## Production Evidence Snapshot

Evidence below was collected from `live.woladen.de` on `2026-04-19` after the decoupled rollout.

### Service Topology

At `2026-04-19 13:34:15 CEST`, systemd showed all three runtime processes active:

- `woladen-live-api.service`
- `woladen-live-ingester.service`
- `woladen-live-queue-worker.service`

The queue directories existed on disk:

- `/var/lib/woladen/live_queue/pending`
- `/var/lib/woladen/live_queue/processing`
- `/var/lib/woladen/live_queue/done`
- `/var/lib/woladen/live_queue/failed`

`/v1/status` at the same stage reported an empty queue:

- `pending_count = 0`
- `processing_count = 0`
- `failed_count = 0`

### Push Receipt Evidence

Fresh push traffic was visible in the API log immediately after restart:

- `2026-04-19 13:34:25 CEST`: `POST /v1/push/qwello` -> `200`
- `2026-04-19 13:35:04 CEST`: `POST /v1/push/ladenetz_de_ladestationsdaten` -> `200`
- `2026-04-19 13:35:18 CEST`: `POST /v1/push/edri` -> `200`

This mattered because the API could acknowledge push delivery without waiting for inline SQLite ingest work.

### Worker Evidence

The queue worker log showed push and poll work interleaving cleanly.

Examples from the same production window:

- `2026-04-19 11:37:25+00:00`: `qwello` processed as `task_kind = push`
- `2026-04-19 11:37:26+00:00`: `chargecloud` processed as `task_kind = poll`
- `2026-04-19 11:37:28+00:00`: `wirelane` processed as `task_kind = poll`
- `2026-04-19 11:38:25+00:00`: `qwello` processed again as `task_kind = push`
- `2026-04-19 11:38:25+00:00`: `tesla` processed as `task_kind = poll`

That is the practical proof that push receipt is no longer serialized through the old inline path.

### Poll Cadence Evidence

After removing the biggest providers from pure round-robin pressure, the remaining pull providers were revisited much faster again.

Examples from queue-worker timestamps:

- `m8mit`: `11:37:21`, `11:37:36`, `11:37:51`, `11:38:06`, `11:38:21`
- `wirelane`: `11:37:28`, `11:37:44`, `11:37:59`, `11:38:14`, `11:38:29`
- `enbwmobility`: `11:37:22`, `11:37:37`, `11:37:53`, `11:38:08`, `11:38:23`

Those are roughly `15-16s` revisit intervals, which is materially better than the earlier multi-minute cadence caused by inline heavy-provider work.

## Operational Lesson

The secret subscription registry is not the only source of truth anymore.

The GitHub deploy workflow regenerates `secret/mobilithek_subscriptions.json` via `scripts/sync_mobilithek_subscriptions.py`. That means delivery-mode overrides must also exist in versioned code, not only in a local secret file. The current source for those live push fallback overrides is `backend/subscriptions.py`.

If this gets forgotten, CI-driven deploys can silently downgrade push-enabled providers back to `poll_only`.

## Current Direction

The current architecture is still intentionally single-host and SQLite-backed.

That is still the correct tradeoff for now because:

- the main failure mode was critical-path coupling, not storage throughput
- the decoupled design materially improved receipt behavior without operational sprawl
- the API only needs station-id lookup against current state, which SQLite serves well on one box

The next scaling lever should be more push adoption and more evidence, not an immediate storage migration.
