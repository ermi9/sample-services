"""Faults a run can ask for, to show what the engine does about them. Only with FAULTS=on.

A run's input names them: {"faults": [{"at": "payment", "kind": "error", "times": 2}]}. "at" is a
service or a step.

  error       answers 503 without doing the work
  lost-reply  does the work, then answers too late
  late-work   starts the work only after the engine gave up on it
  slow        takes "ms" before doing the work
  undo-error  answers an undo with 503
  undo-down   answers every undo with 503 until POST /heal, which heals the undos refused so far
"""
import os
import threading
import time

LONGER_THAN_THE_ENGINE_WAITS = float(os.environ.get("LOST_REPLY_S", "12"))
AFTER_THE_UNDO = float(os.environ.get("LATE_WORK_S", "50"))


class Faults:
    def __init__(self, service):
        self.service = service
        self.paused = False
        self.down = set()
        self.healed = set()
        self.seen = {}
        self.lock = threading.Lock()

    def heal(self):
        self.paused = False
        self.healed |= self.down

    def before_step(self, key, body):
        if self._hit(key, body, "slow", always=True):
            time.sleep(self._hit(key, body, "slow", always=True).get("ms", 1000) / 1000)
        if self._hit(key, body, "late-work"):
            time.sleep(AFTER_THE_UNDO)
        return self.paused or bool(self._hit(key, body, "error"))

    def after_step(self, key, body):
        if self._hit(key, body, "lost-reply"):
            time.sleep(LONGER_THAN_THE_ENGINE_WAITS)

    def before_undo(self, key, body):
        if self._hit(key, body, "undo-down", always=True):
            self.down.add(key)
            if key not in self.healed:
                return True
        return self.paused or bool(self._hit(key, body, "undo-error"))

    def _hit(self, key, body, kind, always=False):
        if os.environ.get("FAULTS") != "on":
            return None
        for fault in body.get("input", {}).get("faults", []):
            if fault.get("kind") != kind or fault.get("at") not in (self.service, body.get("step")):
                continue
            if fault.get("visit", body.get("visit", 1)) != body.get("visit", 1):
                continue
            if always:
                return fault
            with self.lock:
                hits = self.seen[(key, kind)] = self.seen.get((key, kind), 0) + 1
            if hits <= fault.get("times", 1_000_000):
                return fault
        return None
