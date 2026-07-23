# Intermediate 3 — Delivery Semantics & Transactions

Elephant Scale

---

## Agenda

- At-most-once, at-least-once, exactly-once — what each requires
- Transactions and the consume-process-produce loop
- `read_committed` vs. `read_uncommitted`
- Hands-on: build keyed producers/consumers; a transactional pipeline

---

## Three Delivery Guarantees

Every messaging system makes one of three promises about how many times a record is
delivered/processed:

| Guarantee | Meaning | Failure mode |
|---|---|---|
| **At-most-once** | 0 or 1 times | may **lose** records |
| **At-least-once** | 1 or more times | may **duplicate** records |
| **Exactly-once** | exactly 1 time | neither loss nor duplication |

- These aren't a single Kafka setting — they emerge from **producer config + commit timing +
  transactions**
- We've already met the first two; this module builds the third

Notes: Emphasize that "exactly-once" is the hardest and most misunderstood. Half this module is defining precisely what it does and doesn't mean.

---

## Where We Already Are

You've built two of the three, across the last two modules:

- **At-most-once** — auto-commit *before* processing → a crash skips records (Lab 03, Ex 2)
- **At-least-once** — `acks=all` + retries + manual commit *after* processing → no loss, possible
  dups (Lab 02 + Lab 03)
- **Idempotent producer** — removes duplicates from **producer retries** (Lab 02, Ex 5)

What's missing: making a whole **consume → process → produce** step atomic, so the output and
the input-offset commit either **both** happen or **neither** does. That's **transactions**.

---

## The Problem Transactions Solve

A stream processor reads from one topic, transforms, and writes to another — then commits its
read offset. Two writes that must agree:

```
   read "orders"  ──►  process  ──►  write "orders-enriched"
                                 └─►  commit read offset
```

- Crash **after** writing output but **before** committing the offset → on restart you
  **re-read and re-write** → **duplicate output**
- Crash **after** committing the offset but **before** writing → **lost output**

Idempotence alone can't fix this — it spans **two different topics/partitions plus an offset
commit**. You need them to be **one atomic unit**.

---

## Kafka Transactions

A transaction groups **multiple produces across partitions** *and* the **consumer offset
commit** into one all-or-nothing operation.

```
begin_transaction()
  produce(enriched-1)      ┐
  produce(enriched-2)      ├─ all visible together, or not at all
  send_offsets_to_transaction(read offsets)  ┘
commit_transaction()        # ← atomic: outputs + offsets commit together
```

- Either **everything commits** (outputs appear, offset advances) or **everything aborts**
  (outputs discarded, offset unchanged — safe to retry)
- Built on the **idempotent producer** (transactions require it)
- Enables **exactly-once processing** for consume-process-produce pipelines

---

## The `transactional.id`

Transactions need a stable **`transactional.id`** — the producer's durable identity across
restarts.

```python
producer = Producer({
    'bootstrap.servers': 'localhost:9092',
    'transactional.id': 'orders-enricher-1',   # stable across restarts
    'enable.idempotence': True,                 # implied by transactional.id
})
producer.init_transactions()                    # once, at startup
```

- It lets Kafka **fence** a previous (possibly zombie) instance with the same id — the old one
  can no longer commit, so a hung-then-recovered process can't corrupt output
- Must be **stable per logical processor** and **unique per instance** in a parallel deployment

Notes: The transactional.id + fencing is what makes exactly-once safe under the nastiest
failure — a process that froze, was replaced, then woke up. The zombie can't commit.

---

## The Consume-Process-Produce Loop

The canonical exactly-once pattern, end to end:

```python
producer.init_transactions()

while True:
    msgs = consume_batch()
    producer.begin_transaction()
    try:
        for m in msgs:
            result = process(m)
            producer.produce('output', value=result)
        # bind the INPUT offsets to THIS transaction
        producer.send_offsets_to_transaction(
            offsets_of(msgs), consumer.consumer_group_metadata())
        producer.commit_transaction()
    except Exception:
        producer.abort_transaction()            # nothing leaks; safe to retry
```

- The consumer runs with **`enable.auto.commit=False`** — offsets are committed **only via the
  transaction**
- Output records and the input-offset advance are now **one atomic fact**

---

## The Consumer Side: `read_committed`

Transactions on the write side only deliver exactly-once if the **reader** agrees to ignore
uncommitted/aborted data.

```python
consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'downstream',
    'isolation.level': 'read_committed',    # default is read_uncommitted
})
```

| `isolation.level` | Sees |
|---|---|
| `read_uncommitted` (default) | **all** records, including aborted/uncommitted |
| `read_committed` | only records from **committed** transactions |

- `read_committed` readers **never see aborted output** and won't read past an open transaction
  on that partition until it resolves
- Exactly-once end to end = **transactional producer + `read_committed` consumer**

---

## Marches in Lockstep: How read_committed Works

A `read_committed` consumer relies on transaction markers the broker writes into the log.

```
partition log:  [txn1:A][txn1:B][COMMIT txn1][txn2:C][ABORT txn2][txn3:D][COMMIT txn3]
read_committed sees:   A       B                                       D
                    (txn2's C is skipped — aborted; open txns block until resolved)
```

- The broker adds **commit/abort markers**; the consumer uses them to filter
- An **open** transaction holds the reader at the **Last Stable Offset** until it commits or
  aborts — a stuck transaction can add latency (a reason to keep transactions short)

Notes: This is why exactly-once isn't "free" — read_committed trades a little latency for
correctness. Keep transactions small and fast.

---

## What Exactly-Once Really Means (and Doesn't)

Be precise — this is the most over-claimed term in streaming.

- ✅ **Exactly-once *processing*** within Kafka: read Kafka → process → write Kafka, atomically,
  is achievable with transactions
- ❌ It is **not** end-to-end across systems Kafka can't transact with — a write to an external
  DB or a REST call is **not** in the Kafka transaction
- For external side effects you still need **idempotent writes** (upserts, dedupe keys) — the
  effectively-once pattern
- Exactly-once has a **throughput cost**; use it where correctness demands, not everywhere

**Rule of thumb:** exactly-once *inside* Kafka = transactions; exactly-once *touching the
outside world* = idempotent side effects.

---

## Choosing a Guarantee

Match the guarantee to the data — don't pay for more than you need.

- **At-most-once** — high-volume telemetry/logs where a dropped record is harmless
- **At-least-once + idempotent processing** — the **default** for most pipelines; simple and robust
- **Exactly-once (transactions)** — money, counts, ledgers, dedup-critical enrichment — where a
  duplicate or a gap is unacceptable

```
cheaper / faster ───────────────────────► stronger / costlier
 at-most-once     at-least-once     exactly-once
```

Notes: Most students will reach for exactly-once reflexively. Push back: at-least-once +
idempotency covers the majority of real systems at lower cost and complexity.

---

## Summary

- Delivery guarantees come from **producer config + commit timing + transactions**, not one knob
- **At-least-once + idempotent processing** is the sensible default; **exactly-once** is for
  correctness-critical flows
- **Transactions** make *consume → process → produce* atomic: outputs **and** the offset commit
  succeed or abort together
- Require a stable **`transactional.id`** (enables zombie fencing) and the **idempotent producer**
- Readers must use **`read_committed`** to get the guarantee; it filters aborted/uncommitted data
- Exactly-once is **within Kafka**; crossing to external systems needs **idempotent side effects**

---

## Lab 04 Preview

You'll build the full pattern:

- A **transactional producer** and a `read_committed` consumer
- A **consume-process-produce** pipeline that commits offsets *inside* the transaction
- Force an **abort** and prove a `read_committed` reader never sees the aborted records

*→ `labs/04-Transactions/lab-04-transactions.md`*
