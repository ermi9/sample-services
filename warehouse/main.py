"""Two services in one process: inventory holds stock, shipping books couriers. A third, giftwrap,
answers but is not registered, so that it can be plugged in while a flow waits for it."""
import json
import os
import threading
import time
import urllib.request
import uuid

from fastapi import FastAPI

from participant import Participant, Refused


def reserve(db, key, step, body):
    sku, quantity = body["input"].get("sku", "pen"), body["input"].get("quantity", 1)
    if not isinstance(quantity, int) or quantity < 1:
        raise Refused("quantity must be a positive whole number")
    item = db.execute("select * from items where sku = ?", (sku,)).fetchone()
    if not item:
        raise Refused(f"there is no item {sku}")
    if item["stock"] < quantity:
        raise Refused(f"{item['stock']} of {sku} left, {quantity} asked for")
    db.execute("update items set stock = stock - ? where sku = ?", (quantity, sku))
    db.execute("insert into reservations values (?, ?, ?, ?)", (key, body["correlationId"], sku, quantity))
    return {"sku": sku, "reserved": quantity, "left": item["stock"] - quantity, "total": item["price"] * quantity}


def release(db, key, step, body):
    held = db.execute("select * from reservations where key = ?", (key,)).fetchone()
    db.execute("update items set stock = stock + ? where sku = ?", (held["quantity"], held["sku"]))
    db.execute("delete from reservations where key = ?", (key,))


def stock(db):
    return {"stock": {row["sku"]: row["stock"] for row in db.execute("select * from items")},
            "reservations": db.execute("select count(*) from reservations").fetchone()[0]}


inventory = Participant("inventory", """
    create table if not exists items (sku text primary key, stock integer, price integer);
    create table if not exists reservations (key text primary key, run text, sku text, quantity integer);
    insert or ignore into items values ('pen', 1000, 3), ('lamp', 500, 40), ('rare', 1, 900);
    """, reserve, release, stock)


def book(db, key, step, body):
    city = body["input"].get("city", "Wien")
    courier = db.execute("select * from couriers where city = ?", (city,)).fetchone()
    if not courier or courier["free"] < 1:
        raise Refused(f"no courier is free in {city}")
    db.execute("update couriers set free = free - 1 where city = ?", (city,))
    tracking = uuid.uuid4().hex[:10]
    db.execute("insert into bookings values (?, ?, ?, ?)", (key, body["correlationId"], city, tracking))
    return {"city": city, "tracking": tracking}


def cancel(db, key, step, body):
    booking = db.execute("select * from bookings where key = ?", (key,)).fetchone()
    db.execute("update couriers set free = free + 1 where city = ?", (booking["city"],))
    db.execute("delete from bookings where key = ?", (key,))


def bookings(db):
    return {"free": {row["city"]: row["free"] for row in db.execute("select * from couriers")},
            "bookings": db.execute("select count(*) from bookings").fetchone()[0]}


shipping = Participant("shipping", """
    create table if not exists couriers (city text primary key, free integer);
    create table if not exists bookings (key text primary key, run text, city text, tracking text);
    insert or ignore into couriers values ('Wien', 1000), ('Graz', 500), ('Atlantis', 0);
    """, book, cancel, bookings)


def wrap(db, key, step, body):
    db.execute("insert into wrapped values (?)", (key,))
    return {"wrapped": True}


giftwrap = Participant("giftwrap", "create table if not exists wrapped (key text primary key);", wrap,
                       lambda db, key, step, body: db.execute("delete from wrapped where key = ?", (key,)),
                       lambda db: {"wrapped": db.execute("select count(*) from wrapped").fetchone()[0]})

app = FastAPI(title="warehouse")
for participant in (inventory, shipping, giftwrap):
    app.mount("/" + participant.name, participant.app)


def register(name):
    discovery, here = os.environ.get("DISCOVERY_URL"), os.environ.get("PUBLIC_URL", "http://localhost:9000")
    if not discovery:
        return
    body = json.dumps({"name": name, "url": f"{here}/{name}", "openapiUrl": f"{here}/{name}/openapi.json"}).encode()
    while True:
        try:
            urllib.request.urlopen(urllib.request.Request(
                discovery + "/register", body, {"Content-Type": "application/json"}), timeout=5)
            print("registered", name, flush=True)
            return
        except OSError as error:
            print("discovery is not reachable yet:", error, flush=True)
            time.sleep(5)


for registered in ("inventory", "shipping"):
    threading.Thread(target=register, args=(registered,), daemon=True).start()
