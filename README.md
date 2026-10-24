# sandbox
For coding practices
I accepted the invatation

# Video webhook inbox

Python 3.12+ and SQLite, with FastAPI. Processing is simulated by a durable status
transition; no external service is called.

## Run

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
uvicorn app:create_app --factory --host 0.0.0.0 --port 8000
```

The database defaults to `data/inbox.sqlite3` relative to the working directory.
Set `INBOX_DB_PATH` to an absolute path to keep storage independent of the working
directory. Reuse that file across restarts; do not delete it or place it in temporary
storage. Generated database files are excluded from Git.

```bash
curl -i localhost:8000/events -H 'Content-Type: application/json' \
  -d '{"event_id":"evt-1","video_id":"v-1","event_type":"video.ready"}'
curl -i localhost:8000/events/evt-1
curl -i -X POST localhost:8000/events/evt-1/process
```

Delivery accepts exactly three fields. IDs must be strings and are trimmed before
storage and comparison; blank IDs are rejected. Event type must be exactly
`video.ready`. The first delivery returns 201 with status `pending`. Matching
retries return 200 with the current saved state; conflicting payloads return 409.
GET and bodyless processing return 404 for unknown IDs. Processing takes no request body;
any nonempty body (including `{}` or `null`) returns 422 before looking up or updating
the event. Bodyless processing returns 200, including when already processed. Invalid delivery bodies return
422, including unknown fields. Lookup paths use the stored, trimmed event ID.

The non-unique `idx_events_video_id` index supports database lookups by video ID;
startup adds it to existing databases without changing event data. The API currently
exposes retrieval by event ID only.
SQLite's primary key enforces uniqueness. An insert-on-conflict transaction
compares duplicates without overwriting the original. Processing updates only
pending rows within a transaction. Connections are per request, with a 30-second
lock timeout. This ensures one stored inbox record; it does not claim exactly-once
execution of future external side effects.

## Acceptance tests

```bash
python -m pytest -q
```

Tests cover first delivery and direct database persistence, normalized retries,
conflicts before and after processing, repeatable processing, missing IDs, invalid
and malformed bodies, direct database uniqueness, simultaneous matching and
conflicting deliveries through separate app instances, and both statuses after
closing and recreating the app against the same database.
