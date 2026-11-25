// Two services in one process: payment holds customers' balances and loyalty points, orders
// confirms what was paid for and shipped.
import { createServer } from 'node:http';
import { Participant, Refused } from './participant.mjs';

// A step whose id mentions loyalty gives points; any other charges the customer.
const payment = new Participant('payment', `
  create table if not exists accounts (customer text primary key, balance integer, points integer);
  create table if not exists ledger (key text primary key, run text, customer text, amount integer, points integer);
  insert or ignore into accounts values ('ada', 1000000, 0), ('bob', 100, 0), ('eve', 0, 0);`,
(p, key, step, body) => {
  const customer = body.input.customer ?? 'ada';
  const account = p.get('select * from accounts where customer = ?', customer);
  if (!account) { throw new Refused('there is no customer ' + customer); }
  const total = Object.values(body.results).find(result => result?.total !== undefined)?.total ?? body.input.amount ?? 0;
  if (/loyalty/i.test(step)) {
    p.run('update accounts set points = points + ? where customer = ?', total, customer);
    p.run('insert into ledger values (?, ?, ?, 0, ?)', key, body.correlationId, customer, total);
    return { customer, points: account.points + total };
  }
  const parts = body.input.installments ?? 1;
  const amount = body.visit === parts ? total - Math.floor(total / parts) * (parts - 1) : Math.floor(total / parts);
  if (account.balance < amount) { throw new Refused(`${customer} has ${account.balance}, ${amount} is due`); }
  p.run('update accounts set balance = balance - ? where customer = ?', amount, customer);
  p.run('insert into ledger values (?, ?, ?, ?, 0)', key, body.correlationId, customer, amount);
  return { customer, charged: amount, installment: body.visit, remaining: parts - body.visit };
},
(p, key) => {
  const entry = p.get('select * from ledger where key = ?', key);
  p.run('update accounts set balance = balance + ?, points = points - ? where customer = ?',
      entry.amount, entry.points, entry.customer);
  p.run('delete from ledger where key = ?', key);
},
p => ({
  accounts: Object.fromEntries(p.all('select * from accounts').map(a => [a.customer, { balance: a.balance, points: a.points }])),
  entries: p.get('select count(*) as n from ledger').n,
}));

const orders = new Participant('orders', 'create table if not exists orders (key text primary key, run text unique);',
(p, key, step, body) => {
  p.run('insert into orders values (?, ?)', key, body.correlationId);
  return { order: body.correlationId, confirmed: true };
},
(p, key) => p.run('delete from orders where key = ?', key),
p => ({ orders: p.get('select count(*) as n from orders').n }));

const services = { payment, orders };

const read = async request => {
  const chunks = [];
  for await (const chunk of request) { chunks.push(chunk); }
  return chunks.length ? JSON.parse(Buffer.concat(chunks).toString()) : {};
};

createServer(async (request, response) => {
  let status, body;
  try {
    const [, name, path] = request.url.match(/^\/([^/]+)(\/.*)$/) ?? [];
    const service = Object.hasOwn(services, name ?? '') ? services[name] : null;
    [status, body] = service
        ? await service.answer(request.method, path, request.headers['idempotency-key'], await read(request))
        : [404, {}];
  } catch (e) {
    console.error(e);
    [status, body] = [500, { error: String(e.message) }];
  }
  response.writeHead(status, { 'Content-Type': 'application/json' });
  response.end(JSON.stringify(body));
}).listen(9100, '0.0.0.0');

const register = async name => {
  const here = (process.env.PUBLIC_URL ?? 'http://localhost:9100') + '/' + name;
  for (;;) {
    try {
      const answer = await fetch(process.env.DISCOVERY_URL + '/register', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name, url: here, openapiUrl: here + '/openapi.json' }),
      });
      if (answer.ok) { console.log('registered', name); return; }
      console.log('discovery refused', name, answer.status);
    } catch (e) {
      console.log('discovery is not reachable yet:', e.message);
    }
    await new Promise(resolve => setTimeout(resolve, 5000));
  }
};
if (process.env.DISCOVERY_URL) { Object.keys(services).forEach(register); }
