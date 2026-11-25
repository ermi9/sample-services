// Faults a run can ask for; see warehouse/faults.py, which this mirrors. Only with FAULTS=on.
import { setTimeout as sleep } from 'node:timers/promises';

const LONGER_THAN_THE_ENGINE_WAITS = Number(process.env.LOST_REPLY_S ?? 12) * 1000;
const AFTER_THE_UNDO = Number(process.env.LATE_WORK_S ?? 50) * 1000;

export class Faults {
  paused = false;
  down = new Set();
  healed = new Set();
  seen = new Map();

  constructor(service) { this.service = service; }

  heal() { this.paused = false; this.down.forEach(key => this.healed.add(key)); }

  async beforeStep(key, body) {
    const slow = this.hit(key, body, 'slow', true);
    if (slow) { await sleep(slow.ms ?? 1000); }
    if (this.hit(key, body, 'late-work')) { await sleep(AFTER_THE_UNDO); }
    return this.paused || Boolean(this.hit(key, body, 'error'));
  }

  async afterStep(key, body) {
    if (this.hit(key, body, 'lost-reply')) { await sleep(LONGER_THAN_THE_ENGINE_WAITS); }
  }

  beforeUndo(key, body) {
    if (this.hit(key, body, 'undo-down', true)) {
      this.down.add(key);
      if (!this.healed.has(key)) { return true; }
    }
    return this.paused || Boolean(this.hit(key, body, 'undo-error'));
  }

  hit(key, body, kind, always = false) {
    if (process.env.FAULTS !== 'on') { return null; }
    for (const fault of body.input?.faults ?? []) {
      if (fault.kind !== kind || ![this.service, body.step].includes(fault.at)) { continue; }
      if ((fault.visit ?? body.visit ?? 1) !== (body.visit ?? 1)) { continue; }
      if (always) { return fault; }
      const id = key + '|' + kind;
      this.seen.set(id, (this.seen.get(id) ?? 0) + 1);
      if (this.seen.get(id) <= (fault.times ?? Infinity)) { return fault; }
    }
    return null;
  }
}
