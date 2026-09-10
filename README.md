# Assignment 3 — Kafka Order Pipeline with Avro, Retries and a Dead Letter Queue

A Kafka producer/consumer system for **order messages**, built in Python.

Every message is **Avro-serialised** against a schema held in Confluent Schema
Registry, the consumer keeps a **real-time running average of prices**, retries
**transient** failures with exponential backoff, and permanently-failed records
are routed to a **Dead Letter Queue** with full failure context attached.

---

## 1. What is implemented

| Requirement | Where | How |
|---|---|---|
| Avro serialization | [schemas/order.avsc](schemas/order.avsc), [src/producer.py](src/producer.py), [src/consumer.py](src/consumer.py) | Confluent `AvroSerializer` / `AvroDeserializer`, schema registered in Schema Registry, 5-byte wire format (magic byte + schema id + payload) |
| Real-time aggregation | [src/aggregator.py](src/aggregator.py) | Streaming (online) mean — O(1) memory. Overall + per-product, with min/max/stddev |
| Retry logic | [src/consumer.py](src/consumer.py) | Exponential backoff `0.5s → 1s → 2s …` capped at 8s, ±30% jitter, 4 attempts total |
| Dead Letter Queue | [src/consumer.py](src/consumer.py), [schemas/dead_letter.avsc](schemas/dead_letter.avsc) | Failed records republished to `orders.DLQ` as an Avro envelope holding the original bytes + reason + attempt count + source offset |
| DLQ inspection | [src/dlq_consumer.py](src/dlq_consumer.py) | Separate consumer that prints and summarises dead letters |
| Live demo | [run_demo.ps1](run_demo.ps1) | One command: starts the stack and opens producer / consumer / DLQ windows |
| Tests | [src/test_pipeline.py](src/test_pipeline.py) | 16 offline unit tests, no broker required |

---

## 2. The order message

`schemas/order.avsc` — exactly the schema given in the assignment brief:

| Field | Type | Description |
|---|---|---|
| `orderId` | `string` | Unique identifier for the order (e.g. `"1001"`, `"1002"`) |
| `product` | `string` | Name of the purchased item (e.g. `"Item1"`, `"Item2"`) |
| `price` | `float` | Price of the product (randomised) |

```json
{
  "type": "record",
  "name": "Order",
  "namespace": "com.bigdata.assignment3",
  "fields": [
    {"name": "orderId", "type": "string"},
    {"name": "product", "type": "string"},
    {"name": "price",   "type": "float"}
  ]
}
```

Why Avro rather than JSON: the payload is compact (no repeated field names), the
schema is enforced centrally, and Schema Registry blocks an incompatible schema
change from ever reaching the topic.

---

## 3. Architecture

```
                 ┌──────────────────────┐
                 │   Schema Registry    │  (schema id <-> Avro schema)
                 │      :8081           │
                 └───▲──────────────▲───┘
        register     │              │  fetch by id
                     │              │
┌──────────┐   Avro bytes    ┌─────────────────────────────────────┐
│ producer ├────────────────►│           topic: orders             │
└──────────┘                 └──────────────────┬──────────────────┘
                                                │
                                                ▼
                                    ┌───────────────────────┐
                                    │      consumer         │
                                    │                       │
                                    │  deserialize ─┐       │
                                    │  validate  ───┤ fail  │
                                    │  process   ───┘       │
                                    │      │                │
                                    │  transient? ── retry  │
                                    │      │  (backoff x4)  │
                                    │      ▼                │
                                    │  running average      │
                                    └───────┬───────────────┘
                                            │ permanent failure
                                            ▼
                                 ┌───────────────────────┐
                                 │  topic: orders.DLQ    │──► dlq_consumer.py
                                 └───────────────────────┘
```

### Retry vs. Dead Letter — the decision rule

The whole design hinges on classifying a failure ([src/errors.py](src/errors.py)):

* **`TransientError`** — could plausibly succeed next time (network blip,
  downstream 503, DB deadlock). → **retry with backoff**.
* **`PermanentError`** — the message itself is wrong (undecodable bytes,
  negative price, blank product). No number of retries helps. → **straight to
  the DLQ**.

Retrying a permanent failure just burns the topic's throughput; dead-lettering a
transient failure loses data that would have worked. Getting this split right is
the point of the exercise.

### Delivery guarantees

* Producer: `acks=all` + `enable.idempotence=true` — no lost or duplicated
  writes on broker-side retries.
* Consumer: `enable.auto.commit=false`. The offset is committed **only after**
  the record is either processed or durably written to the DLQ
  (`dlq.flush()` blocks before the commit). This yields **at-least-once**
  processing — a crash mid-record replays it rather than dropping it.
* `max.poll.interval.ms=600000` gives the blocking retry backoffs room to run
  without the broker evicting the consumer from the group.

---

## 4. Prerequisites

* Docker Desktop (running)
* Python 3.10+

```powershell
pip install -r requirements.txt
```

---

## 5. Running it

### Quick path — one command

```powershell
.\run_demo.ps1
```

Starts Kafka + Schema Registry, waits for readiness, creates the topics, then
opens three windows: consumer, DLQ inspector, producer.

### Manual path — four terminals

```powershell
# 0. infrastructure
docker compose up -d
python src/create_topics.py

# 1. consumer (terminal 1)
python src/consumer.py --from-beginning

# 2. DLQ inspector (terminal 2)
python src/dlq_consumer.py --from-beginning

# 3. producer (terminal 3)
python src/producer.py -n 60 -i 0.4 --corrupt-rate 0.05
```

Tear down when finished:

```powershell
docker compose down -v
```

### Useful flags

**Producer**

| Flag | Meaning |
|---|---|
| `-n, --count` | number of orders (`0` = run until Ctrl+C) |
| `-i, --interval` | seconds between messages |
| `--poison-rate` | fraction with invalid business data (negative price / blank product) |
| `--corrupt-rate` | fraction emitted as non-Avro garbage bytes |
| `--start-id` | first `orderId` |

**Consumer**

| Flag | Meaning |
|---|---|
| `--from-beginning` | read the topic from offset 0 |
| `--max-attempts` | attempts before dead-lettering (default 4) |
| `--transient-rate` | simulated transient failure probability (`0` disables) |
| `--report-every` | print the full aggregation table every N orders |
| `--max-records` | exit cleanly after N records are settled (`0` = until Ctrl+C) |
| `--idle-timeout` | exit after N seconds with no new records (`0` = wait forever) |
| `--group` | consumer group id — run two consumers in the same group to show partition rebalancing |

**DLQ inspector**

| Flag | Meaning |
|---|---|
| `--from-beginning` | read every dead letter ever written |
| `--show-payload` | also print the raw bytes of the original record |
| `--idle-timeout` | exit after N seconds with no new dead letters |
| `--group` | consumer group id |

> The DLQ inspector commits its offsets, so a second run in the same group shows
> only *new* dead letters. Pass a fresh `--group` name to re-read the topic from
> the start.

> When redirecting output to a file, run with `python -u` — otherwise Python
> buffers stdout and the log stays empty until the process exits.

---

## 6. What the demo shows

**Normal processing with a live running average**

```
[consumer] OK    order=1001   product=Item3  price=  412.55 | running_avg=  412.55 | orders=1 avg=  412.55 ...
[consumer] OK    order=1002   product=Item1  price=   88.10 | running_avg=  250.33 | orders=2 avg=  250.33 ...
```

**A transient failure being retried, then succeeding**

```
[consumer] RETRY order=1007 attempt 1/4 in 0.44s (downstream order service temporarily unavailable)
[consumer] RETRY order=1007 attempt 2/4 in 1.13s (downstream order service temporarily unavailable)
[consumer] OK    order=1007   product=Item2  price=  145.90 | running_avg=  233.71 ...
```

**A poison message going to the DLQ on the first attempt**

```
[producer] INJECTED POISON  {'orderId': '1011', 'product': 'Item4', 'price': -233.4}
[consumer] -> DLQ  key=1011 attempts=1 PermanentError: price must be positive, got -233.40
```

**Retries exhausted → DLQ**

```
[consumer] RETRY order=1018 attempt 3/4 in 2.31s (...)
[consumer] -> DLQ  key=1018 attempts=4 PermanentError: still failing after 4 attempts: ...
```

**The DLQ inspector, with full forensic context**

```
[dlq] #3 orderId=1011 attempts=1 at 2026-09-10 09:41:22
      reason : PermanentError: price must be positive, got -233.40
      origin : orders[2]@37
```

**Periodic aggregation table**

```
[consumer] === running aggregation ===
--------------------------------------------------------------
PRODUCT   COUNT        AVG       MIN       MAX        TOTAL
--------------------------------------------------------------
Item1        11     251.34     11.02    486.77      2764.74
Item2         9     238.90      7.45    492.11      2150.10
...
ALL          52     249.61      5.12    499.88     12979.72
--------------------------------------------------------------
```

Kafka UI at <http://localhost:8080> shows the topics, the registered schemas and
the messages — useful to display during the live demo.

### A verified run

Producing 45 orders with `--poison-rate 0.12 --corrupt-rate 0.07`:

```
[producer] done: sent=45 good=32 poison=5 corrupt=8
[consumer] processed=31 retries=11 dead_lettered=14
[dlq]      total dead letters seen: 14
```

`31 processed + 14 dead-lettered = 45 produced` — nothing lost, nothing
duplicated. The 14 dead letters covered every failure path:

* 8 × `Avro deserialization failed` (the corrupt, non-Avro bytes)
* 3 × `price must be positive` (poison)
* 2 × `product name is empty` (poison)
* 1 × `still failing after 4 attempts` (a *good* order whose transient failures
  never cleared — the retry budget was exhausted)

The other 10 retried orders recovered on a later attempt and were aggregated
normally, which is exactly the distinction the design is built around.

---

## 7. Tests

```powershell
python src/test_pipeline.py
```

16 tests, no broker needed. They cover the streaming average (verified against a
batch average and a known standard deviation), numerical stability over 100k
values, the validation rules, backoff growth/cap/non-negativity, and that
`order.avsc` matches the schema in the assignment brief.

---

## 8. Project layout

```
.
├── docker-compose.yml        Kafka (KRaft) + Schema Registry + Kafka UI
├── requirements.txt
├── run_demo.ps1              one-command live demo
├── schemas/
│   ├── order.avsc            the assignment's order schema
│   └── dead_letter.avsc      DLQ envelope schema
└── src/
    ├── config.py             all settings, env-var overridable
    ├── create_topics.py      creates orders + orders.DLQ
    ├── producer.py           Avro producer + failure injection
    ├── consumer.py           retry + DLQ + aggregation
    ├── aggregator.py         streaming running average
    ├── dlq_consumer.py       DLQ inspector
    ├── errors.py             Transient vs Permanent taxonomy
    └── test_pipeline.py      offline unit tests
```

## 9. Configuration

Every setting in [src/config.py](src/config.py) can be overridden by an
environment variable — `KAFKA_BOOTSTRAP_SERVERS`, `SCHEMA_REGISTRY_URL`,
`ORDERS_TOPIC`, `DLQ_TOPIC`, `MAX_ATTEMPTS`, `RETRY_BASE_DELAY`,
`RETRY_MAX_DELAY`, `TRANSIENT_FAILURE_RATE`, `POISON_MESSAGE_RATE` — so the same
code runs against any cluster without a code change.
