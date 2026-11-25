"""Runs every compensation case against the live system and checks the services' own data.

After a case that must be undone, stock, balances, points, couriers and orders are what they were
before it. After one that completes, they changed by exactly one order. Usage: showcase.py [case ...]
"""
import concurrent.futures
import glob
import json
import os
import random
import sys
import time
import urllib.error
import urllib.request

ENGINE = os.environ.get("ENGINE_URL", "http://engine:8080")
DISCOVERY = os.environ.get("DISCOVERY_URL", "http://discovery:8080")
WAREHOUSE = os.environ.get("WAREHOUSE_URL", "http://warehouse:9000")
SHOP = os.environ.get("SHOP_URL", "http://shop:9100")
KAFKA = os.environ.get("KAFKA", "kafka:9092")
SERVICES = {"inventory": WAREHOUSE, "shipping": WAREHOUSE, "giftwrap": WAREHOUSE, "payment": SHOP, "orders": SHOP}


def call(method, url, body=None, content_type="application/json"):
    data = body.encode() if isinstance(body, str) else None if body is None else json.dumps(body).encode()
    request = urllib.request.Request(url, data, {"Content-Type": content_type}, method=method)
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            return json.loads(response.read() or b"null")
    except urllib.error.HTTPError as error:
        raise RuntimeError(f"{method} {url}: {error.code} {error.read()[:300]!r}") from error


def state():
    return {name: call("GET", f"{base}/{name}/state") for name, base in SERVICES.items()}


def events(run):
    found = [event for name, base in SERVICES.items() for event in call("GET", f"{base}/{name}/events/{run}")]
    return sorted(found, key=lambda event: event["at"])


def start(flow, **given):
    return call("POST", f"{ENGINE}/api/flows/{flow}/runs", {"customer": "ada", "sku": "pen", "quantity": 2, **given})["correlationId"]


def outcome(run):
    return call("GET", f"{ENGINE}/api/runs/{run}")


def until(what, test, within=120):
    deadline = time.time() + within
    while time.time() < deadline:
        if test():
            return
        time.sleep(0.5)
    raise AssertionError(f"{what} did not happen within {within} s")


def ends(run, status, within=120):
    until(f"run {run} being {status}", lambda: outcome(run)["status"] == status, within)
    return outcome(run)


def same(before, within=30):
    """The services' data is what it was; a late branch may take a moment to be undone."""
    until("everything being put back", lambda: state() == before, within)


def did(run, what, service=None):
    return [e for e in events(run) if e["what"] == what and service in (None, e["service"])]


def in_order(run, *services):
    undone = [event["service"] for event in did(run, "undone")]
    assert undone == list(services), f"undone in order {undone}, expected {list(services)}"


def outcomes(run):
    return {entry["step"]: entry["outcome"] for entry in outcome(run)["trace"]}


def fault(at, kind, **more):
    return [{"at": at, "kind": kind, **more}]


def happy():
    """Everything works: one order of 2 pens for 6 changes every service's data once."""
    before = state()
    ends(start("checkout"), "COMPLETED")
    after = state()
    assert after["inventory"]["stock"]["pen"] == before["inventory"]["stock"]["pen"] - 2
    ada, was = after["payment"]["accounts"]["ada"], before["payment"]["accounts"]["ada"]
    assert (was["balance"] - ada["balance"], ada["points"] - was["points"]) == (6, 6)
    assert after["orders"]["orders"] == before["orders"]["orders"] + 1
    assert after["shipping"]["bookings"] == before["shipping"]["bookings"] + 1


def first_step_refused():
    """Out of stock at the first step: nothing was done, so nothing is undone."""
    before = state()
    run = start("pickup", sku="rare", quantity=2)
    ends(run, "COMPENSATED")
    assert not did(run, "undone")
    same(before)


def later_step_refused():
    """Not enough money: the refusal is not retried, and the stock goes back."""
    before = state()
    run = start("pickup", customer="eve")
    ends(run, "COMPENSATED")
    in_order(run, "inventory")
    assert len([e for e in events(run) if e["service"] == "payment"]) == 1
    same(before)


def last_step_fails():
    """The last step fails: payment is undone before inventory."""
    before = state()
    run = start("pickup", faults=fault("orders", "error"))
    ends(run, "COMPENSATED")
    in_order(run, "payment", "inventory")
    same(before)


def branch_fails():
    """No courier in Atlantis: the finished sibling branch is undone latest first, then the step before the fork."""
    before = state()
    run = start("checkout", city="Atlantis", faults=fault("shipping", "slow", ms=2500))
    ends(run, "COMPENSATED")
    in_order(run, "payment", "payment", "inventory")
    undone = [event["step"] for event in did(run, "undone")]
    assert undone[:2] == ["Task_loyalty", "Task_charge"], undone
    same(before)


def sibling_skipped():
    """A branch fails while its sibling is charging: the sibling's next step is skipped, not run and undone."""
    before = state()
    run = start("checkout", city="Atlantis", faults=fault("Task_charge", "slow", ms=4000))
    ends(run, "COMPENSATED")
    assert outcomes(run)["Task_loyalty"] == "SKIPPED", outcomes(run)
    in_order(run, "payment", "inventory")
    same(before)


def after_the_join():
    """The step after a join fails: both branches are undone, and only then the step before the fork."""
    before = state()
    run = start("checkout", faults=fault("orders", "error"))
    ends(run, "COMPENSATED")
    undone = [event["service"] for event in did(run, "undone")]
    assert sorted(undone) == ["inventory", "payment", "payment", "shipping"] and undone[-1] == "inventory", undone
    same(before)


def passing_error():
    """Two errors, then it works: one charge, nothing undone."""
    before = state()
    run = start("pickup", faults=fault("payment", "error", times=2))
    ends(run, "COMPLETED")
    assert len(did(run, "done", "payment")) == 1
    assert state()["payment"]["entries"] == before["payment"]["entries"] + 1


def reply_lost_once():
    """The charge is made but its answer is lost: the retry is answered from memory, so ada pays once."""
    before = state()
    run = start("pickup", faults=fault("payment", "lost-reply", times=1))
    ends(run, "COMPLETED")
    assert len(did(run, "done", "payment")) == 1 and did(run, "repeat answered from memory", "payment")
    assert before["payment"]["accounts"]["ada"]["balance"] - state()["payment"]["accounts"]["ada"]["balance"] == 6


def reply_always_lost():
    """The charge is made and no answer ever arrives: uncertain, so it is undone too, before the stock."""
    before = state()
    run = start("pickup", faults=fault("payment", "lost-reply"))
    ends(run, "COMPENSATED")
    assert outcomes(run)["payment"] == "UNCERTAIN", outcomes(run)
    in_order(run, "payment", "inventory")
    same(before)


def work_after_undo():
    """Attempts that come to life after they were undone are refused, so the charge never happens."""
    before = state()
    run = start("pickup", faults=fault("payment", "late-work"))
    ends(run, "COMPENSATED")
    assert did(run, "undo of nothing", "payment")
    until("the late attempts arriving", lambda: len(did(run, "refused: already undone", "payment")) == 3)
    assert not did(run, "done", "payment")
    same(before)


def undo_fails_then_works():
    """An undo fails twice and then works: the steps before it wait for it."""
    before = state()
    run = start("pickup", faults=fault("orders", "error") + fault("payment", "undo-error", times=2))
    ends(run, "COMPENSATED")
    in_order(run, "payment", "inventory")
    same(before)


def undo_parked():
    """An undo never works: it is parked and inventory waits, until a person fixes it and tries again."""
    before = state()
    run = start("pickup", faults=fault("orders", "error") + fault("payment", "undo-down"))
    ends(run, "PARKED", 400)
    assert not did(run, "undone"), "something was undone past the parked undo"
    call("POST", f"{SHOP}/payment/heal")
    call("POST", f"{ENGINE}/api/runs/{run}/undo", {})
    ends(run, "COMPENSATED")
    in_order(run, "payment", "inventory")
    same(before)


def join_times_out():
    """A branch is too slow: what arrived is undone, and the late branch undoes its own work when it comes."""
    before = state()
    run = start("checkout-hurry", faults=fault("shipping", "slow", ms=8000))
    ends(run, "COMPENSATED")
    until("the late branch being undone", lambda: did(run, "undone", "shipping"), 40)
    undone = [event["service"] for event in did(run, "undone")]
    assert undone.count("inventory") == 1 and undone[-1] == "shipping", undone
    same(before)


def loop_completes():
    """Three installments: the loop charges three times, and 3 pens cost 9 in all."""
    before = state()
    run = start("installments", quantity=3, installments=3)
    ends(run, "COMPLETED")
    assert len(did(run, "done", "payment")) == 3
    assert before["payment"]["accounts"]["ada"]["balance"] - state()["payment"]["accounts"]["ada"]["balance"] == 9


def loop_fails_midway():
    """bob can pay two of three installments of 40: both are undone, latest first, then the stock."""
    before = state()
    run = start("installments", customer="bob", sku="lamp", quantity=3, installments=3)
    ends(run, "COMPENSATED")
    in_order(run, "payment", "payment", "inventory")
    keys = [event["key"] for event in did(run, "done", "payment")]
    assert [event["key"] for event in did(run, "undone", "payment")] == keys[::-1], "rounds undone out of order"
    same(before)


def loop_limit():
    """Seven installments where the flow allows five rounds: it stops, and five rounds are undone."""
    before = state()
    run = start("installments", quantity=7, installments=7)
    ends(run, "COMPENSATED")
    assert len(did(run, "undone", "payment")) == 5
    same(before)


def delivered_twice():
    """Kafka delivers a step and an undo a second time: the work is done once and undone once."""
    from kafka import KafkaConsumer, KafkaProducer

    def redeliver(topic, run):
        consumer = KafkaConsumer(topic, bootstrap_servers=KAFKA, auto_offset_reset="earliest",
                                 group_id=None, consumer_timeout_ms=8000)
        record = next((r for r in consumer if r.key and r.key.decode() == run), None)
        consumer.close()
        assert record is not None, f"no record of {run} on {topic}"
        producer = KafkaProducer(bootstrap_servers=KAFKA)
        producer.send(topic, key=record.key, value=record.value, headers=record.headers).get(10)
        producer.close()

    run = start("pickup")
    ends(run, "COMPLETED")
    done = state()
    redeliver("payment.in", run)
    until("the second delivery arriving", lambda: did(run, "repeat answered from memory", "payment"))
    time.sleep(3)
    assert state() == done, "a second delivery changed something"

    before = state()
    run = start("pickup", faults=fault("orders", "error"))
    ends(run, "COMPENSATED")
    same(before)
    redeliver("payment.compensate", run)
    until("the second undo arriving", lambda: did(run, "repeated undo", "payment"))
    time.sleep(3)
    same(before)


def late_service_never_comes():
    """A flow reaches a service nobody registered: it waits, gives up and undoes."""
    before = state()
    run = start("gift")
    ends(run, "COMPENSATED")
    in_order(run, "inventory")
    same(before)


def late_service_comes():
    """The missing service is plugged in while the flow waits for it: the same flow completes."""
    run = start("gift")
    time.sleep(4)
    here = os.environ.get("GIFTWRAP_URL", WAREHOUSE) + "/giftwrap"
    call("POST", f"{DISCOVERY}/register", {"name": "giftwrap", "url": here, "openapiUrl": here + "/openapi.json"})
    try:
        ends(run, "COMPLETED")
        assert did(run, "done", "giftwrap")
    finally:
        call("DELETE", f"{DISCOVERY}/services/giftwrap")


def live(service):
    return any(s["name"] == service and s["live"] for s in call("GET", f"{ENGINE}/api/catalog"))


def service_down_then_back():
    """payment is down when the flow reaches it and comes back within the wait: the flow completes."""
    call("POST", f"{SHOP}/payment/pause")
    try:
        until("discovery noticing", lambda: not live("payment"))
        run = start("pickup")
        time.sleep(5)
        assert not did(run, "done", "payment")
    finally:
        call("POST", f"{SHOP}/payment/heal")
    ends(run, "COMPLETED")
    until("payment being live again", lambda: live("payment"))


def service_stays_down():
    """payment is down and stays down past the wait: the stock goes back."""
    before = state()
    call("POST", f"{SHOP}/payment/pause")
    try:
        until("discovery noticing", lambda: not live("payment"))
        run = start("pickup")
        ends(run, "COMPENSATED")
        in_order(run, "inventory")
    finally:
        call("POST", f"{SHOP}/payment/heal")
    until("payment being live again", lambda: live("payment"))
    same(before)


def many_at_once():
    """60 checkouts at once, each with a random fault or none: exactly the completed ones show in the data."""
    seed = int(os.environ.get("SEED", "0")) or random.randrange(1, 10**6)
    dice = random.Random(seed)
    print(f"      seed {seed}")
    before = state()
    choices = [[], [], []] + [fault(at, kind, **more) for at in ("inventory", "payment", "shipping", "orders", "Task_loyalty")
                              for kind, more in (("error", {}), ("error", {"times": 1}))]
    runs = [start("checkout", faults=dice.choice(choices) + (fault("payment", "undo-error", times=1) if dice.random() < .2 else []),
                  city=dice.choice(["Wien", "Graz", "Atlantis"]) if dice.random() < .3 else "Wien") for _ in range(60)]
    with concurrent.futures.ThreadPoolExecutor(10) as pool:
        def finish(run):
            until(f"run {run} ending", lambda: outcome(run)["status"] in ("COMPLETED", "COMPENSATED"), 600)
            return outcome(run)["status"]
        ended = list(pool.map(finish, runs))
    n = ended.count("COMPLETED")
    print(f"      {n} completed, {len(runs) - n} compensated")
    expected = json.loads(json.dumps(before))
    expected["inventory"]["stock"]["pen"] -= 2 * n
    expected["inventory"]["reservations"] += n
    expected["payment"]["accounts"]["ada"]["balance"] -= 6 * n
    expected["payment"]["accounts"]["ada"]["points"] += 6 * n
    expected["payment"]["entries"] += 2 * n
    expected["orders"]["orders"] += n
    expected["shipping"]["bookings"] += n

    def matches():
        now = state()
        free = sum(before["shipping"]["free"].values()) - sum(now["shipping"]["free"].values())
        return free == n and {**now, "shipping": {**now["shipping"], "free": before["shipping"]["free"]}} == expected
    until("the data matching the outcomes", matches, 60)


ALL_IN_ONE = "order-with-giftwrap-installments-and-parallel-delivery"
TWO_FORKS = "charge-and-reward-then-ship-then-aftercare-two-forks"


def steps(run, what):
    return [event["step"] for event in did(run, what)]


def put_back_after(run, before):
    """Compensated, every step done was undone once, reserve last, and the data is what it was."""
    ends(run, "COMPENSATED")
    same(before)
    done, undone = steps(run, "done"), steps(run, "undone")
    assert sorted(done) == sorted(undone), f"done {done}, undone {undone}"
    assert not done or undone[-1] == "Task_reserve" or "Task_reserve" not in done, undone
    return undone


def all_happy():
    """[all in one] Two installments, Graz: the loop runs twice and the branch's choice books a second courier."""
    run = start(ALL_IN_ONE, city="Graz", installments=2)
    ends(run, "COMPLETED")
    done = steps(run, "done")
    assert done.count("Task_charge") == 2 and "Task_second_leg" in done and "giftwrap" not in done, done
    assert done[-2:] == ["orders", "Task_loyalty_bonus"], done


def all_branch_refused():
    """[all in one] Atlantis: everything before the fork is undone, both installments included."""
    before = state()
    run = start(ALL_IN_ONE, city="Atlantis", installments=2)
    undone = put_back_after(run, before)
    assert undone.count("Task_charge") == 2, undone
    assert undone.index("Task_loyalty") < undone.index("Task_charge"), undone


def all_sibling_skipped():
    """[all in one] The courier is refused while the spare is being reserved: its points step is skipped."""
    before = state()
    run = start(ALL_IN_ONE, city="Atlantis", faults=fault("Task_spare", "slow", ms=3000))
    put_back_after(run, before)
    assert outcomes(run)["Task_loyalty_spare"] == "SKIPPED", outcomes(run)


def all_loop_fails():
    """[all in one] The second of three installments fails: the first is undone, then the stock."""
    before = state()
    run = start(ALL_IN_ONE, installments=3, faults=fault("Task_charge", "error", visit=2))
    assert put_back_after(run, before) == ["Task_charge", "Task_reserve"]


def all_loop_limit():
    """[all in one] Seven installments where five are allowed: all five are undone."""
    before = state()
    run = start(ALL_IN_ONE, quantity=7, installments=7)
    assert put_back_after(run, before) == ["Task_charge"] * 5 + ["Task_reserve"]


def all_gift_never_comes():
    """[all in one] A large order needs the giftwrap service, which nobody registered: the stock goes back."""
    before = state()
    run = start(ALL_IN_ONE, sku="lamp", quantity=10)
    assert put_back_after(run, before) == ["Task_reserve"]


def all_gift_comes():
    """[all in one] giftwrap is plugged in while the run waits for it, and a later failure unwraps the gift."""
    before = state()
    run = start(ALL_IN_ONE, sku="lamp", quantity=10, installments=2, faults=fault("Task_loyalty_bonus", "error"))
    time.sleep(4)
    here = os.environ.get("GIFTWRAP_URL", WAREHOUSE) + "/giftwrap"
    call("POST", f"{DISCOVERY}/register", {"name": "giftwrap", "url": here, "openapiUrl": here + "/openapi.json"})
    try:
        undone = put_back_after(run, before)
        assert undone[-2:] == ["giftwrap", "Task_reserve"], undone
    finally:
        call("DELETE", f"{DISCOVERY}/services/giftwrap")


def all_join_times_out():
    """[all in one] The courier is booked after the join gave up: the late branch undoes its own booking."""
    before = state()
    run = start(ALL_IN_ONE, faults=fault("Task_book", "slow", ms=9000))
    ends(run, "COMPENSATED")
    until("the late booking being undone", lambda: "Task_book" in steps(run, "undone"), 40)
    same(before)
    done, undone = steps(run, "done"), steps(run, "undone")
    assert sorted(done) == sorted(undone), f"done {done}, undone {undone}"


def all_after_the_join():
    """[all in one] The last step fails: the order, then both branches, then everything before the fork."""
    before = state()
    run = start(ALL_IN_ONE, city="Graz", faults=fault("Task_loyalty_bonus", "error"))
    undone = put_back_after(run, before)
    assert undone[0] == "orders" and undone[-3:] == ["Task_loyalty", "Task_charge", "Task_reserve"], undone
    assert undone.index("Task_second_leg") < undone.index("Task_book"), undone


def all_reply_lost():
    """[all in one] The charge is made but never confirmed: uncertain, and undone."""
    before = state()
    run = start(ALL_IN_ONE, faults=fault("Task_charge", "lost-reply"))
    assert put_back_after(run, before) == ["Task_charge", "Task_reserve"]
    assert outcomes(run)["Task_charge"] == "UNCERTAIN", outcomes(run)


def all_undo_parked():
    """[all in one] Undoing the courier never works: earlier steps wait until a person has it tried again."""
    before = state()
    run = start(ALL_IN_ONE, faults=fault("Task_loyalty_bonus", "error") + fault("Task_book", "undo-down"))
    ends(run, "PARKED", 400)
    assert not {"Task_loyalty", "Task_charge", "Task_reserve"} & set(steps(run, "undone")), steps(run, "undone")
    call("POST", f"{WAREHOUSE}/shipping/heal")
    call("POST", f"{ENGINE}/api/runs/{run}/undo", {})
    put_back_after(run, before)


def forks_happy():
    """[two forks] Both joins fire once, and every step runs once."""
    run = start(TWO_FORKS)
    ends(run, "COMPLETED")
    done = steps(run, "done")
    assert len(done) == len(set(done)) == 7, done


def forks_first_refused():
    """[two forks] eve cannot pay: the sibling's points are undone, and the second fork is never reached."""
    before = state()
    run = start(TWO_FORKS, customer="eve")
    put_back_after(run, before)
    assert "shipping" not in steps(run, "done")


def forks_last_fails():
    """[two forks] The last step fails: second fork, courier, first fork, stock, in that order."""
    before = state()
    run = start(TWO_FORKS, faults=fault("orders", "error"))
    undone = put_back_after(run, before)
    assert (set(undone[:2]), undone[2], set(undone[3:5]), undone[5]) == (
        {"Task_loyalty_bonus", "Task_spare"}, "shipping", {"Task_charge", "Task_loyalty"}, "Task_reserve"), undone


CASES = [happy, first_step_refused, later_step_refused, last_step_fails, branch_fails, sibling_skipped, after_the_join,
         passing_error, reply_lost_once, reply_always_lost, work_after_undo, undo_fails_then_works, undo_parked,
         join_times_out, loop_completes, loop_fails_midway, loop_limit, delivered_twice, late_service_never_comes,
         late_service_comes, service_down_then_back, service_stays_down, many_at_once,
         all_happy, all_branch_refused, all_sibling_skipped, all_loop_fails, all_loop_limit, all_gift_never_comes,
         all_gift_comes, all_join_times_out, all_after_the_join, all_reply_lost, all_undo_parked,
         forks_happy, forks_first_refused, forks_last_fails]


def main():
    wanted = sys.argv[1:]
    for path in sorted(glob.glob("flows/*.bpmn")):
        with open(path) as flow:
            call("PUT", f"{ENGINE}/api/flows/{os.path.basename(path)[:-5]}", flow.read(), "application/xml")
    until("the services being live", lambda: all(live(s) for s in ("inventory", "payment", "shipping", "orders")))
    failed = []
    cases = [case for case in CASES if not wanted or case.__name__ in wanted]
    for case in cases:
        print(f"{case.__name__}: {case.__doc__}")
        began = time.time()
        try:
            case()
            print(f"  ok  {time.time() - began:.0f} s")
        except Exception as error:
            failed.append(case.__name__)
            print(f"  FAILED after {time.time() - began:.0f} s: {error!r}")
    print(f"\n{len(cases) - len(failed)} of {len(cases)} cases passed" + (f"; failed: {failed}" if failed else ""))
    sys.exit(1 if failed or not cases else 0)


if __name__ == "__main__":
    main()
