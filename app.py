"""A durable, idempotent webhook inbox. Processing is a local state transition."""

import os
import sqlite3
from contextlib import closing
from pathlib import Path
from typing import Annotated, Literal

from fastapi import Depends, FastAPI, HTTPException, Request, Response
from pydantic import BaseModel, ConfigDict, StringConstraints


Identifier = Annotated[str, StringConstraints(strict=True, strip_whitespace=True, min_length=1)]


class Delivery(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: Identifier
    video_id: Identifier
    event_type: Literal["video.ready"]


class Event(Delivery):
    status: Literal["pending", "processed"]


def create_app(database_path: str | Path | None = None) -> FastAPI:
    path = Path(database_path or os.environ.get("INBOX_DB_PATH", "data/inbox.sqlite3"))
    path.parent.mkdir(parents=True, exist_ok=True)

    def connect():
        connection = sqlite3.connect(path, timeout=30)
        connection.row_factory = sqlite3.Row
        return connection

    with closing(connect()) as connection, connection:
        connection.execute("""
            CREATE TABLE IF NOT EXISTS events (
                event_id TEXT PRIMARY KEY NOT NULL,
                video_id TEXT NOT NULL,
                event_type TEXT NOT NULL CHECK (event_type = 'video.ready'),
                status TEXT NOT NULL CHECK (status IN ('pending', 'processed'))
            )
        """)
        connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_events_video_id ON events(video_id)"
        )

    app = FastAPI(title="Video webhook inbox")

    @app.post("/events", response_model=Event, status_code=201)
    def receive(delivery: Delivery, response: Response):
        with closing(connect()) as connection, connection:
            # The unique key arbitrates races. A write transaction stays open
            # through the lookup, so a duplicate is compared to the saved row.
            cursor = connection.execute(
                """INSERT INTO events (event_id, video_id, event_type, status)
                   VALUES (?, ?, ?, 'pending') ON CONFLICT(event_id) DO NOTHING""",
                (delivery.event_id, delivery.video_id, delivery.event_type),
            )
            row = connection.execute(
                "SELECT * FROM events WHERE event_id = ?", (delivery.event_id,)
            ).fetchone()
            if cursor.rowcount == 0:
                if row["video_id"] != delivery.video_id or row["event_type"] != delivery.event_type:
                    raise HTTPException(409, "Event ID already has a different payload")
                response.status_code = 200
            return dict(row)

    @app.get("/events/{event_id}", response_model=Event)
    def retrieve(event_id: str):
        with closing(connect()) as connection:
            row = connection.execute(
                "SELECT * FROM events WHERE event_id = ?", (event_id,)
            ).fetchone()
            if row is None:
                raise HTTPException(404, "Event not found")
            return dict(row)

    async def require_empty_body(request: Request):
        if await request.body():
            raise HTTPException(422, "Processing does not accept a request body")

    @app.post("/events/{event_id}/process", response_model=Event,
              dependencies=[Depends(require_empty_body)])
    def process(event_id: str):
        with closing(connect()) as connection, connection:
            connection.execute(
                "UPDATE events SET status = 'processed' WHERE event_id = ? AND status = 'pending'",
                (event_id,),
            )
            row = connection.execute(
                "SELECT * FROM events WHERE event_id = ?", (event_id,)
            ).fetchone()
            if row is None:
                raise HTTPException(404, "Event not found")
            return dict(row)

    return app
