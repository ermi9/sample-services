// The participant contract on Node's own HTTP server and SQLite, with no dependencies. The same
// rules as warehouse/participant.py: a key's work and its answer are one transaction, and a key
// that was undone is never done again.
import { mkdirSync } from 'node:fs';
import { join } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { Faults } from './faults.mjs';

export class Refused extends Error {}

const operation = summary => ({
  summary,
  parameters: [
    { name: 'step', in: 'path', required: true, schema: { type: 'string' } },
    { name: 'Idempotency-Key', in: 'header', required: true, schema: { type: 'string' } }],
  requestBody: { content: { 'application/json': { schema: { type: 'object' } } } },
  responses: { 200: { description: 'done' } },
});

export class Participant {
  constructor(name, schema, act, undo, state) {
    Object.assign(this, { name, act, undo, state, faults: new Faults(name) });
    const folder = process.env.DATA ?? 'data';
    mkdirSync(folder, { recursive: true });
    this.db = new DatabaseSync(join(folder, name + '.db'));
    this.db.exec(`create table if not exists runs (key text primary key, status text, result text);
      create table if not exists events (id integer primary key, at real, run text, step text, what text, key text);
      ${schema}`);
  }

  // Node runs this without interruption, so one connection and one transaction at a time suffice.
  transaction(work) {
    this.db.exec('begin immediate');
    try {
      const result = work();
      this.db.exec('commit');
      return result;
    } catch (e) {
      this.db.exec('rollback');
      throw e;
    }
  }

  get(sql, ...values) { return this.db.prepare(sql).get(...values); }
  all(sql, ...values) { return this.db.prepare(sql).all(...values); }
  run(sql, ...values) { return this.db.prepare(sql).run(...values); }

  note(body, what, key) {
    this.run('insert into events (at, run, step, what, key) values (?, ?, ?, ?, ?)',
        Date.now() / 1000, body.correlationId ?? null, body.step ?? null, what, key);
  }

  step(key, step, body) {
    try {
      return this.transaction(() => {
        const known = this.get('select * from runs where key = ?', key);
        if (known?.status === 'undone') {
          this.note(body, 'refused: already undone', key);
          return [409, { reason: 'this run was already undone' }];
        }
        if (known) {
          this.note(body, 'repeat answered from memory', key);
          return [200, JSON.parse(known.result)];
        }
        const result = this.act(this, key, step, body);
        this.run("insert into runs values (?, 'done', ?)", key, JSON.stringify(result));
        this.note(body, 'done', key);
        return [200, result];
      });
    } catch (e) {
      if (!(e instanceof Refused)) { throw e; }
      this.note(body, 'refused: ' + e.message, key);
      return [422, { reason: e.message }];
    }
  }

  undoStep(key, step, body) {
    return this.transaction(() => {
      const known = this.get('select * from runs where key = ?', key);
      if (known?.status === 'done') {
        this.undo(this, key, step, body);
        this.note(body, 'undone', key);
      } else {
        this.note(body, known ? 'repeated undo' : 'undo of nothing', key);
      }
      this.run("insert into runs values (?, 'undone', null) on conflict (key) do update set status = 'undone'", key);
      return [200, {}];
    });
  }

  spec() {
    return {
      openapi: '3.0.3',
      info: { title: this.name, version: '1' },
      paths: {
        '/choreography/steps/{step}': { post: operation('Do a step') },
        '/choreography/undo/{step}': { post: operation('Undo a step') },
      },
    };
  }

  async answer(method, path, key, body) {
    const [, kind, step] = path.match(/^\/choreography\/(steps|undo)\/([^/]+)$/) ?? [];
    if (method === 'POST' && kind) {
      if (!key) { return [400, { reason: 'Idempotency-Key is missing' }]; }
      const name = decodeURIComponent(step);
      if (kind === 'undo') {
        return this.faults.beforeUndo(key, body) ? [503, {}] : this.undoStep(key, name, body);
      }
      if (await this.faults.beforeStep(key, body)) { return [503, {}]; }
      const answer = this.step(key, name, body);
      await this.faults.afterStep(key, body);
      return answer;
    }
    if (method === 'GET' && path === '/health') {
      return this.faults.paused ? [503, { status: 'DOWN' }] : [200, { status: 'UP' }];
    }
    if (method === 'GET' && path === '/openapi.json') { return [200, this.spec()]; }
    if (method === 'GET' && path === '/state') { return [200, this.state(this)]; }
    if (method === 'GET' && path.startsWith('/events/')) {
      return [200, this.all('select at, step, what, key from events where run = ? order by id',
          decodeURIComponent(path.slice('/events/'.length))).map(row => ({ service: this.name, ...row }))];
    }
    if (method === 'POST' && path === '/pause') { this.faults.paused = true; return [200, {}]; }
    if (method === 'POST' && path === '/heal') { this.faults.heal(); return [200, {}]; }
    return [404, {}];
  }
}
