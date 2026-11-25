# Sample services

Services that take part in the [EDA choreography engine](https://github.com/ermi9/EDA-choreography)
by implementing its [participant contract](https://github.com/ermi9/EDA-choreography/tree/main/contract).
They use no library of the engine and never see Kafka. Each has its own SQLite database.

| Service | Process | Language | A step | Its undo |
|---|---|---|---|---|
| `inventory` | warehouse | Python | takes items from stock | puts them back |
| `shipping` | warehouse | Python | books a courier in a city | frees the courier |
| `payment` | shop | JavaScript | charges a customer, or gives loyalty points | refunds, or takes the points back |
| `orders` | shop | JavaScript | confirms the order | deletes it |
| `giftwrap` | warehouse | Python | wraps | unwraps; registered only by the showcase |

## Write your own

Copy `warehouse/participant.py` or `shop/participant.mjs`. They keep the contract's rules: a key's
work and its answer are one transaction, and an undone key is never done again. You supply a
function that does a step and one that undoes it, as `warehouse/main.py` and `shop/main.mjs` do.

## Run them

With the engine's compose stack running on the same machine:

```sh
FAULTS=on docker compose up -d
docker compose run --rm showcase            # every case
docker compose run --rm showcase branch_fails undo_parked
```

The services register with discovery, and the engine takes them up.

## The showcase

It runs each case below and compares the services' own data before and after. `FAULTS=on` lets a
run's input ask a service to misbehave (`warehouse/faults.py`). It is for demonstrations: leave it
off elsewhere.

| Case | What it shows |
|---|---|
| `happy` | a checkout changes every service once |
| `first_step_refused`, `later_step_refused`, `last_step_fails` | undo along a line, refusals without retries |
| `branch_fails` | a finished sibling branch is undone before the step ahead of the fork |
| `sibling_skipped` | a branch stops starting steps once its sibling failed |
| `after_the_join` | both branches are undone, then the step before the fork |
| `passing_error` | an error that passes is retried |
| `reply_lost_once` | the work was done but the answer lost: charged once |
| `reply_always_lost` | nobody knows whether it was charged: undone either way |
| `work_after_undo` | an attempt that comes to life after its undo is refused |
| `undo_fails_then_works`, `undo_parked` | an undo is retried, then parked until a person tries again |
| `join_times_out` | a late branch undoes its own work |
| `loop_completes`, `loop_fails_midway`, `loop_limit` | loops, undone round by round |
| `delivered_twice` | Kafka delivers a step and an undo twice |
| `late_service_comes`, `late_service_never_comes` | a service plugged in while a flow waits for it, or never |
| `service_down_then_back`, `service_stays_down` | a service that is down when the flow reaches it |
| `many_at_once` | 60 checkouts at once with random faults |

Two larger flows put the cases together:

| Flow | Cases | What is in it |
|---|---|---|
| `order-with-giftwrap-installments-and-parallel-delivery` | `all_*` | a choice, a late-bound service, a loop with a limit, a fork with a choice in a branch, a join with a timeout, services that run several steps, a step after the join |
| `charge-and-reward-then-ship-then-aftercare-two-forks` | `forks_*` | two forks in a row, the first with both branches at one service |

A step is called after its service where the flow has one task on it, and after the task's id otherwise.

By hand, in the editor: run `checkout` with `{"sku": "pen", "quantity": 2, "city": "Atlantis"}` to
see a branch fail, since no courier serves Atlantis.
