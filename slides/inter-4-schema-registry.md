# Intermediate 4 — Serialization & Schema Registry

Elephant Scale

---

## Agenda

- Message formats: JSON, Avro, Protobuf
- The Schema Registry: registering schemas, schema IDs, serdes
- Schema evolution and compatibility (BACKWARD / FORWARD / FULL)
- Data contracts as a governance practice
- Hands-on: produce/consume Avro with the Schema Registry; evolve a schema

---

## Kafka Stores Bytes — Someone Must Agree on Meaning

From the producer module: Kafka moves **bytes**. The producer serializes; the consumer
deserializes. Nothing in the broker enforces what those bytes *mean*.

```
producer  {claimId: 42, amount: 100}  ──serialize──►  10101110...  ──►  Kafka
Kafka  ──►  10101110...  ──deserialize──►  {claimId: 42, amount: 100}  consumer
```

- If producer and consumer disagree on the shape, the consumer reads **garbage** — or crashes
- With **several systems** producing and consuming the same topics, "just agree informally"
  doesn't scale
- You need a **contract** on the data's shape — enforced, versioned, shared

That contract is a **schema**, and the Schema Registry is where it lives.

Notes:
Tie this straight to their world — multiple systems sharing appointment/claim events. Informal agreement breaks the first time one team changes a field.

---

## Message Formats

Three common ways to structure event payloads:

| Format | Schema | Size | Notes |
|---|---|---|---|
| **JSON** | none (or JSON Schema) | large (text) | human-readable, no enforcement by default |
| **Avro** | required (`.avsc`) | small (binary) | compact, strong evolution rules, Kafka's most common choice |
| **Protobuf** | required (`.proto`) | small (binary) | cross-language, popular for gRPC shops |

- Plain JSON is easy but **unenforced** — every consumer hopes the fields are there
- **Avro / Protobuf** carry a real schema, are compact on the wire, and support **safe evolution**
- This course uses **Avro** with the Schema Registry — the mainstream Kafka pattern

---

## The Schema Registry

A standalone service (in our lab at `:8081`) that stores schemas and hands out IDs.

```
producer ──register schema──► Schema Registry ──returns──► schema ID (e.g. 7)
producer sends:  [magic byte][schema id 7][avro bytes]  ──► Kafka
consumer reads:  [magic byte][schema id 7][avro bytes]
consumer ──"what is schema 7?"──► Schema Registry ──returns──► the schema
```

- The **schema travels by ID**, not inline — a few bytes, not the whole schema per message
- Producers **auto-register** schemas; consumers **fetch by ID** and cache them
- One source of truth for "what's in this topic," shared across all systems

---

## Serdes: How Your Code Uses It

You don't hand-code the wire format — a **serializer/deserializer (serde)** does it. In the
Kafka **Java** client that's Confluent's Avro serde:

```java
// Producer side
props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
props.put("schema.registry.url", "http://localhost:8081");
// produce a GenericRecord / SpecificRecord — the serde registers & encodes it

// Consumer side
props.put("value.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");
props.put("schema.registry.url", "http://localhost:8081");
props.put("specific.avro.reader", "true");   // deserialize into generated classes
```

- The serializer registers the schema (if new) and prefixes the ID
- The deserializer reads the ID, fetches/caches the schema, and decodes
- Your business code sees **objects**, not bytes

Notes:
The magic is that "schema management" becomes two config lines plus a serde. Students should see it's not extra plumbing they maintain.

---

## Subjects and Versions

The registry organizes schemas into **subjects**, each with a version history.

- Default naming: a topic `claims` has subjects `claims-value` (and `claims-key` if keyed)
- Each subject holds an ordered list of **versions**: v1, v2, v3, …
- A **compatibility rule** is attached per subject and governs which new versions are allowed

```
subject "claims-value"
  v1  {claimId, amount}
  v2  {claimId, amount, region}        ← must be compatible with v1
  v3  {claimId, amount, region, tier}  ← must be compatible per the rule
```

The registry **rejects** a new version that violates the subject's compatibility rule — that's
the enforcement that keeps producers and consumers from breaking each other.

---

## Schema Evolution — The Real Problem

Systems change independently. A producer adds a field; a consumer hasn't been updated yet — or
vice versa. **Evolution rules** decide whether that's safe.

Two directions to reason about:

- **Can a NEW consumer read OLD data?** (you upgraded the reader first)
- **Can an OLD consumer read NEW data?** (the writer changed first)

Because Kafka **retains** the log, a topic can hold **old and new versions at once** — a
consumer replaying history will meet both. So evolution isn't optional; it's the normal state.

Notes:
Callback to Intro 2 retention: the log holds mixed versions. This is exactly the SSA "when do we upgrade" question — it's really "which compatibility mode."

---

## Compatibility Modes

The subject's mode defines what changes are allowed and who can upgrade first.

| Mode | Guarantees | New schema may | Upgrade first |
|---|---|---|---|
| **BACKWARD** (default) | new consumer reads old data | **delete** fields, **add optional** | **consumers** |
| **FORWARD** | old consumer reads new data | **add** fields, **delete optional** | **producers** |
| **FULL** | both directions | add/remove **optional** only | either |
| **NONE** | no checks | anything (dangerous) | — |

- **BACKWARD** is the default and most common: upgrade consumers, they still read the old log
- **FORWARD** when producers must move first and old consumers must tolerate new data
- **FULL** for the strictest cross-team contracts

Notes:
The "who upgrades first" column is the practical takeaway. Match the mode to your rollout order — this directly answers their upgrade-timing concern.

---

## The Golden Rule: Add With Defaults

The one habit that keeps evolution safe under BACKWARD/FULL:

- **Adding a field?** Give it a **default value** → old data (missing the field) still deserializes
- **Removing a field?** Only safe if it **had a default** → consumers expecting it fall back
- **Renaming / changing a type?** Usually **breaking** — use aliases or a new field instead

```json
{ "name": "region", "type": "string", "default": "UNKNOWN" }
```

- A new consumer reading an old record with no `region` gets `"UNKNOWN"` — no crash
- This single discipline (defaults on new fields) covers the majority of real evolutions

---

## Data Contracts as Governance

The Schema Registry is a **governance tool**, not just a codec. For an organization with many
systems sharing events, it's how teams stay coordinated **without** a change-control meeting.

- The schema **is the contract** between producing and consuming systems
- Compatibility rules let teams **deploy independently** — no lock-step, coordinated-outage upgrades
- Breaking changes are **caught at registration**, in CI, not in production at 2 AM
- New consumers can be added anytime — they read the schema and understand the topic

**Business value:** independent teams, safe change, no big-bang upgrade windows.

Notes:
This slide is the "why leadership cares" framing — decoupled deploys and no coordinated outages map directly to their current pain of scheduling upgrade windows.

---

## Lab 05 — What You'll Do (Java)

- Define an **Avro schema** and produce records with `KafkaAvroSerializer`
- Inspect the registered schema via the **registry REST API** (`:8081`)
- Consume with `KafkaAvroDeserializer`
- **Evolve** the schema (add a field with a default) under **BACKWARD** compatibility — prove old
  consumers still work
- Attempt an **incompatible** change and watch the registry **reject** it

*→ `labs/05-Schema-Registry/lab-05-schema-registry.md`*

---

## Summary

- Kafka moves bytes; a **schema** is the enforced contract on their meaning
- **Avro + Schema Registry** is the mainstream pattern: schemas travel **by ID**, serdes do the work
- Schemas live as **subjects** with **versions** and a **compatibility rule** that the registry enforces
- **BACKWARD / FORWARD / FULL** decide what changes are legal and **who upgrades first**
- **Add fields with defaults** — the golden rule for safe evolution
- The registry is **governance**: independent deploys, safe change, no coordinated-outage upgrades
