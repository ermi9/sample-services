"""The participant contract on FastAPI and SQLite.

A service gives a function that does a step and one that undoes it. This keeps the keys: each
key's work and its answer are committed in one transaction, so a repeat is answered from memory,
and a key that was undone is never done again.
"""
import json
import os
import sqlite3
import time
from contextlib import contextmanager

from fastapi import FastAPI, Header, Response
from fastapi.responses import JSONResponse

from faults import Faults


class Refused(Exception):
    """The service will not do the step, and has done nothing."""


class Participant:
    def __init__(self, name, schema, do, undo, state):
        self.name, self.do, self.undo, self.state = name, do, undo, state
        os.makedirs(os.environ.get("DATA", "data"), exist_ok=True)
        self.file = os.path.join(os.environ.get("DATA", "data"), name + ".db")
        self.faults = Faults(name)
        with self.transaction() as db:
            db.execute("create table if not exists runs (key text primary key, status text, result text)")
            db.execute("create table if not exists events "
                       "(id integer primary key, at real, run text, step text, what text, key text)")
        with self.connection() as db:
            db.executescript(schema)
        self.app = FastAPI(title=name, version="1")
        self._routes()

    @contextmanager
    def connection(self):
        db = sqlite3.connect(self.file, timeout=10, isolation_level=None)
        db.row_factory = sqlite3.Row
        try:
            yield db
        finally:
            db.close()

    @contextmanager
    def transaction(self):
        with self.connection() as db:
            db.execute("begin immediate")
            try:
                yield db
                db.execute("commit")
            except BaseException:
                db.execute("rollback")
                raise

    def note(self, db, body, what, key):
        db.execute("insert into events (at, run, step, what, key) values (?, ?, ?, ?, ?)",
                   (time.time(), body.get("correlationId"), body.get("step"), what, key))

    def step(self, key, step, body):
        try:
            with self.transaction() as db:
                known = db.execute("select * from runs where key = ?", (key,)).fetchone()
                if known and known["status"] == "undone":
                    self.note(db, body, "refused: already undone", key)
                    return 409, {"reason": "this run was already undone"}
                if known:
                    self.note(db, body, "repeat answered from memory", key)
                    return 200, json.loads(known["result"])
                result = self.do(db, key, step, body)
                db.execute("insert into runs values (?, 'done', ?)", (key, json.dumps(result)))
                self.note(db, body, "done", key)
                return 200, result
        except Refused as refusal:
            with self.transaction() as db:
                self.note(db, body, "refused: " + str(refusal), key)
            return 422, {"reason": str(refusal)}

    def undo_step(self, key, step, body):
        with self.transaction() as db:
            known = db.execute("select * from runs where key = ?", (key,)).fetchone()
            if known and known["status"] == "done":
                self.undo(db, key, step, body)
                self.note(db, body, "undone", key)
            else:
                self.note(db, body, "undo of nothing" if not known else "repeated undo", key)
            db.execute("insert into runs values (?, 'undone', null) "
                       "on conflict (key) do update set status = 'undone'", (key,))
        return {}

    def _routes(self):
        app = self.app

        @app.get("/health")
        def health():
            return JSONResponse({"status": "DOWN"}, 503) if self.faults.paused else {"status": "UP"}

        @app.post("/choreography/steps/{step}")
        def do_step(step: str, body: dict, idempotency_key: str = Header()):
            if self.faults.before_step(key=idempotency_key, body=body):
                return Response(status_code=503)
            status, answer = self.step(idempotency_key, step, body)
            self.faults.after_step(key=idempotency_key, body=body)
            return JSONResponse(answer, status)

        @app.post("/choreography/undo/{step}")
        def undo_step(step: str, body: dict, idempotency_key: str = Header()):
            if self.faults.before_undo(key=idempotency_key, body=body):
                return Response(status_code=503)
            return self.undo_step(idempotency_key, step, body)

        @app.get("/state")
        def state():
            with self.connection() as db:
                return self.state(db)

        @app.get("/events/{run}")
        def events(run: str):
            with self.connection() as db:
                return [{"service": self.name, **dict(row)} for row in
                        db.execute("select at, step, what, key from events where run = ? order by id", (run,))]

        @app.post("/pause")
        def pause():
            self.faults.paused = True
            return {}

        @app.post("/heal")
        def heal():
            self.faults.heal()
            return {}
