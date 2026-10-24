import sqlite3
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

import pytest
from fastapi.testclient import TestClient

from app import create_app


PAYLOAD = {"event_id": "evt-1", "video_id": "v-1", "event_type": "video.ready"}


def test_video_id_index_supports_lookup_and_multiple_events(tmp_path):
    path = tmp_path / "indexed.sqlite3"
    # Simulate an existing database created before the index was introduced.
    with sqlite3.connect(path) as connection:
        connection.execute("CREATE TABLE events (event_id TEXT PRIMARY KEY NOT NULL, "
                           "video_id TEXT NOT NULL, event_type TEXT NOT NULL, status TEXT NOT NULL)")
        connection.execute("INSERT INTO events VALUES ('old', 'v-1', 'video.ready', 'pending')")
    for _ in range(2):
        with TestClient(create_app(path)) as client:
            assert client.get("/events/old").json()["status"] == "pending"
    with TestClient(create_app(path)) as client:
        assert client.post("/events", json=PAYLOAD).status_code == 201
    with sqlite3.connect(path) as connection:
        plan = connection.execute(
            "EXPLAIN QUERY PLAN SELECT * FROM events WHERE video_id = ?", ("v-1",)
        ).fetchall()
        assert any("USING INDEX idx_events_video_id" in step[3] for step in plan)
        assert connection.execute("SELECT count(*) FROM events WHERE video_id = 'v-1'").fetchone()[0] == 2


@pytest.fixture
def inbox(tmp_path):
    path = tmp_path / "inbox.sqlite3"
    with TestClient(create_app(path)) as client:
        yield client, path


def rows(path):
    with sqlite3.connect(path) as connection:
        return connection.execute("SELECT * FROM events ORDER BY event_id").fetchall()


def test_first_delivery_is_persisted_pending(inbox):
    client, path = inbox
    response = client.post("/events", json=PAYLOAD)
    assert response.status_code == 201
    assert response.json() == {**PAYLOAD, "status": "pending"}
    assert rows(path) == [("evt-1", "v-1", "video.ready", "pending")]
    assert client.get("/events/evt-1").json() == response.json()


def test_normalized_retry_returns_stored_state_without_duplicate(inbox):
    client, path = inbox
    first = client.post("/events", json={**PAYLOAD, "event_id": " evt-1 ", "video_id": "\tv-1\n"})
    assert first.status_code == 201
    retry = client.post("/events", json=PAYLOAD)
    assert retry.status_code == 200
    assert retry.json() == first.json()
    assert len(rows(path)) == 1


@pytest.mark.parametrize("processed", [False, True])
def test_conflict_cannot_overwrite_original(inbox, processed):
    client, path = inbox
    client.post("/events", json=PAYLOAD)
    if processed:
        client.post("/events/evt-1/process")
    before = rows(path)
    response = client.post("/events", json={**PAYLOAD, "video_id": "v-2"})
    assert response.status_code == 409
    assert rows(path) == before


def test_process_is_repeatable_and_delivery_returns_processed_state(inbox):
    client, path = inbox
    client.post("/events", json=PAYLOAD)
    first = client.post("/events/evt-1/process")
    assert first.status_code == 200
    assert first.json() == {**PAYLOAD, "status": "processed"}
    before = rows(path)
    again = client.post("/events/evt-1/process")
    assert again.status_code == 200
    assert again.json() == first.json()
    retry = client.post("/events", json=PAYLOAD)
    assert retry.status_code == 200
    assert retry.json() == first.json()
    assert rows(path) == before


def test_unknown_ids_return_404(inbox):
    client, path = inbox
    assert client.get("/events/missing").status_code == 404
    assert client.post("/events/missing/process").status_code == 404
    assert rows(path) == []


@pytest.mark.parametrize("body", [b'{}', b'{"status":"processed"}', b'null', b'[]', b'"text"', b'42', b'{', b' '])
@pytest.mark.parametrize("state", ["pending", "processed", "missing"])
def test_process_rejects_bodies_without_changing_data(inbox, body, state):
    client, path = inbox
    if state != "missing":
        client.post("/events", json=PAYLOAD)
        if state == "processed":
            client.post("/events/evt-1/process")
    before = rows(path)
    response = client.post("/events/evt-1/process", content=body,
                           headers={"Content-Type": "application/json"})
    assert response.status_code == 422
    assert rows(path) == before


@pytest.mark.parametrize("body", [
    {}, None, [], "invalid",
    {**PAYLOAD, "extra": True},
    {"event_id": "evt-1", "video_id": "v-1"},
    {"video_id": "v-1", "event_type": "video.ready"},
    {"event_id": "evt-1", "event_type": "video.ready"},
    *[{**PAYLOAD, key: value} for key in ["event_id", "video_id"]
      for value in ["", " \t\n", None, 1, True, [], {}]],
    *[{**PAYLOAD, "event_type": value} for value in ["video.failed", " video.ready ", "", None, 1]],
])
def test_invalid_bodies_return_422_without_writes(inbox, body):
    client, path = inbox
    assert client.post("/events", json=body).status_code == 422
    assert rows(path) == []


def test_malformed_json_returns_422(inbox):
    client, path = inbox
    assert client.post("/events", content='{', headers={"Content-Type": "application/json"}).status_code == 422
    assert rows(path) == []


def test_database_enforces_unique_event_id(inbox):
    client, path = inbox
    client.post("/events", json=PAYLOAD)
    with sqlite3.connect(path) as connection:
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute("INSERT INTO events VALUES (?, ?, ?, ?)",
                               ("evt-1", "v-2", "video.ready", "pending"))
    assert rows(path) == [("evt-1", "v-1", "video.ready", "pending")]


@pytest.mark.parametrize("conflicting", [False, True])
def test_simultaneous_deliveries_across_app_instances(tmp_path, conflicting):
    path = tmp_path / "shared.sqlite3"
    apps = [create_app(path) for _ in range(8)]
    barrier = Barrier(len(apps))

    def deliver(index):
        payload = {**PAYLOAD, "video_id": f"v-{index}"} if conflicting else PAYLOAD
        with TestClient(apps[index]) as client:
            barrier.wait(timeout=10)
            response = client.post("/events", json=payload)
            return response.status_code, response.json()

    with ThreadPoolExecutor(max_workers=len(apps)) as pool:
        results = list(pool.map(deliver, range(len(apps))))
    codes = [code for code, _ in results]
    assert codes.count(201) == 1
    assert codes.count(409 if conflicting else 200) == 7
    assert len(rows(path)) == 1
    winner = next(body for code, body in results if code == 201)
    assert rows(path)[0] == (winner["event_id"], winner["video_id"], "video.ready", "pending")
    if not conflicting:
        assert all(body == winner for _, body in results)


def test_pending_and_processed_survive_restart(tmp_path):
    path = tmp_path / "durable.sqlite3"
    pending = {**PAYLOAD, "event_id": "evt-pending"}
    with TestClient(create_app(path)) as client:
        client.post("/events", json=PAYLOAD)
        client.post("/events", json=pending)
        client.post("/events/evt-1/process")
    with TestClient(create_app(path)) as restarted:
        assert restarted.get("/events/evt-1").json() == {**PAYLOAD, "status": "processed"}
        assert restarted.get("/events/evt-pending").json() == {**pending, "status": "pending"}
        assert restarted.post("/events", json=PAYLOAD).status_code == 200
        assert restarted.post("/events/evt-1/process").json()["status"] == "processed"
    assert len(rows(path)) == 2


def test_simultaneous_processing_is_retry_safe(inbox):
    client, path = inbox
    client.post("/events", json=PAYLOAD)
    apps = [create_app(path) for _ in range(8)]
    barrier = Barrier(len(apps))

    def process(app):
        with TestClient(app) as worker:
            barrier.wait(timeout=10)
            response = worker.post("/events/evt-1/process")
            assert response.status_code == 200
            return response.json()

    with ThreadPoolExecutor(max_workers=len(apps)) as pool:
        results = list(pool.map(process, apps))
    assert all(result == {**PAYLOAD, "status": "processed"} for result in results)
    assert rows(path) == [("evt-1", "v-1", "video.ready", "processed")]


def test_persistence_across_python_processes(tmp_path):
    path = tmp_path / "process-restart.sqlite3"
    setup = '''
import sys
from fastapi.testclient import TestClient
from app import create_app
with TestClient(create_app(sys.argv[1])) as client:
    for event_id in ['pending', 'processed']:
        assert client.post('/events', json=dict(event_id=event_id, video_id='v-1', event_type='video.ready')).status_code == 201
    assert client.post('/events/processed/process').status_code == 200
'''
    verify = '''
import sys
from fastapi.testclient import TestClient
from app import create_app
with TestClient(create_app(sys.argv[1])) as client:
    for event_id in ['pending', 'processed']:
        response = client.get('/events/' + event_id)
        assert response.status_code == 200
        assert response.json()['status'] == event_id
        retry = client.post('/events', json=dict(event_id=event_id, video_id='v-1', event_type='video.ready'))
        assert retry.status_code == 200
        assert retry.json() == response.json()
'''
    for script in [setup, verify]:
        subprocess.run([sys.executable, "-c", script, str(path)], check=True, timeout=20)
    assert len(rows(path)) == 2
