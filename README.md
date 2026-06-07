# **E**xchange via **Z**ero-loss **R**elay **A**gent


<p align="center">
  <a href="https://github.com/entGriff/ezra/actions/workflows/test.yml"><img src="https://github.com/entGriff/ezra/actions/workflows/test.yml/badge.svg" alt="CI"></a>
  <a href="https://github.com/entGriff/ezra/releases"><img src="https://img.shields.io/github/v/release/entGriff/ezra" alt="Latest release"></a>
  <a href="https://github.com/entGriff/ezra/blob/main/LICENSE"><img src="https://img.shields.io/github/license/entGriff/ezra" alt="License"></a>
</p>

EZRA is a persistent task queue. Multiple services push tasks in, multiple workers pull them out and confirm when done.
Each task stays visible and explicitly tracked until a worker marks it finished - no silent drops, no fire-and-forget. Backed by SQLite, powered by the Erlang/OTP runtime. Workers connect with any Redis client (Redis itself is not needed) in any language - no new SDK required.

> **This project is maintained by a single author and pull requests are not accepted. Issues for bugs or questions are welcome.**

---

## Contents

- [Quick start](#quick-start)
- [The big picture](#the-big-picture)
- [Why does this exist?](#why-does-this-exist)
- [How it works](#how-it-works)
- [Task lifecycle](#task-lifecycle)
- [When things go wrong](#when-things-go-wrong)
- [Multiple workers and producers](#multiple-workers-and-producers)
- [Trade-offs](#trade-offs)
- [Is EZRA right for you?](#is-ezra-right-for-you)
- [Install](#install)
- [Run](#run)
- [Elixir](#elixir)
- [Terminology](#terminology)

---

## Quick start

```bash
docker run -d --name ezra \
  -p 42002:42002 \
  -v ezra_data:/data \
  ghcr.io/entgriff/ezra
```

That is the entire server setup. Now, from any machine that can reach that port:

**Producer** - push a task

```python
import redis

r = redis.Redis(host="localhost", port=42002, decode_responses=True, protocol=3)

# Push a task into the "emails" queue.
# Queues do not need to be created in advance - the first push creates one.
r.xadd("emails", {"payload": '{"to": "alice@example.com"}'})
```

**Worker** - pop and process tasks

```python
import redis

r = redis.Redis(host="localhost", port=42002, decode_responses=True, protocol=3)

while True:
    # Ask Ezra for the next task from "emails".
    # "workers"       - consumer group name, required by the wire protocol but ignored by Ezra.
    # "worker-1"      - this worker's identity (each process needs a unique name).
    # {"emails": ">"} - give me the next undelivered task from this queue.
    # block=0         - wait indefinitely; Ezra delivers the task the moment one arrives.
    results = r.xreadgroup("workers", "worker-1", {"emails": ">"}, count=1, block=0)

    if results:
        for task_id, fields in results["emails"][0]:
            send_email(fields["payload"])  # your processing code here

            # Acknowledge success. Without this, Ezra re-delivers the task after the
            # visibility timeout (default 30 seconds).
            r.xack("emails", "workers", task_id)
```

Any language with a Redis client works the same way - Python, Node.js, Go, Ruby, Java. Point the client at port 42002 instead of Redis.

For runnable Docker Compose demos in Python and Node.js, see **[github.com/entGriff/ezra-examples](https://github.com/entGriff/ezra-examples/)**.

---

## The big picture

![EZRA overview](docs/diagrams/overview.svg)

Services and workers can run on any machine in any language. Workers actively pull tasks when ready - Ezra delivers one immediately if available, or holds the connection until one arrives. Everything persists to `ezra.db` on the server.

| | |
|---|---|
| Memory per connected worker | ~2 KB (not an OS thread) |
| Memory baseline | ~20 MB |
| Throughput on a typical cloud VM (SSD) | ~15k–30k tasks/sec |
| Throughput on NVMe | ~40k–80k tasks/sec |
| Binary size | ~20 MB, self-contained |

Throughput is bounded by SQLite write speed, which depends on the disk. The engine itself adds ~1–5 µs overhead per call.

---

## Why does this exist?

At some point almost all app needs to do work outside the request cycle - send an email, generate a PDF, call a slow API. You want to return to the user immediately, process it in the background, retry if it fails, and not lose it when the server restarts. That is where task queue helps.

There are some battle tested options, but they come with real overhead and overengineering if you do not have 1 million RPS: clusters to provision, dedicated servers to maintain, operational knowledge to acquire, or a dependency on a cloud provider. Most teams end up skipping persistent queuing entirely and using in-memory jobs that silently lose work on restart.

EZRA is the alternative that does not feel heavy. One binary, one SQLite file, any Redis client in any language. No broker, no cluster, no pre-configuration. Run it and you can open the database in any SQLite browser and see exactly what is in the queue.

---

## How it works

EZRA speaks **RESP3** - the same wire protocol Redis uses. Every Redis client library in every language already knows how to speak it. Point the client at EZRA's port instead of Redis and it works without modification.

The specific commands EZRA implements come from **Redis Streams** - the part of Redis built around the idea that a message must be explicitly acknowledged before it is considered done:

- **XADD** - push a task into a named queue
- **XREADGROUP** - pop the next task and claim it under a worker identity; supports blocking so workers do not need to poll
- **XACK** - confirm that a task was processed successfully
- **XDEL** - report failure; EZRA returns the task for retry instead of deleting it
- **XNACK** - same as XDEL, for clients whose SDK can send arbitrary commands directly

Everything else Redis supports (`GET`, `SET`, pub/sub, etc.) returns an error. EZRA is not trying to be Redis.

---

## Task lifecycle

```mermaid
stateDiagram-v2
    [*] --> available : push
    available --> in_flight : pop
    in_flight --> available : crash, timeout, or nack with retries left
    in_flight --> done : ack
    in_flight --> dead : nack or timeout, no retries left
    dead --> [*] : readable via queue&#58;&#58;dead
```

**Tasks are never silently lost.** A task stays in the queue until a worker explicitly says it is done. If EZRA restarts, in-flight tasks are returned to `available` immediately on startup before any new work is accepted.

**After a nack, can the same worker get the same task again?** Yes. When a task is nacked it returns to `available` and the next pop - from any worker, including the same one - can claim it. If you want to avoid tight retry loops, add a short sleep in your worker between a failure and the next pop. The `last_error` field stores the nack reason for inspection.

---

## When things go wrong

**Worker crashes or disconnects mid-task.** The task stays `in_flight`. The scheduler reclaims it after `visibility_timeout` seconds (default: 30) and puts it back in `available`, incrementing `attempts`.

**EZRA itself crashes.** Workers see a TCP disconnect. On restart, EZRA immediately resets all in-flight tasks back to `available` before accepting new connections. A task that was mid-processing when the crash happened may run again - this is at-least-once delivery by design, not a failure mode. No tasks are lost.

**Task fails repeatedly.** After `max_attempts` (default: 3) the task moves to `dead` and lands in `<queue>::dead`, queryable via the same `XREADGROUP` interface. Nothing is silently dropped.

**Worker is slow.** If a worker takes longer than `visibility_timeout` to ack, the task is reclaimed and redelivered to another worker. Set `visibility_timeout` per queue to match your workload - size it for the worst case, not the average.

---

## Multiple workers and producers

EZRA exposes a network API over TCP. Any machine that can reach the port can push tasks or pop them. No registration, no configuration per client - just connect and use. See [The big picture](#the-big-picture) for a visual overview.

- Any number of producer clients can push to the same queue simultaneously
- Any number of worker clients can pop from the same queue - each task goes to exactly one worker, never duplicated
- Workers are identified by a unique name you provide (`worker-1`, `worker-2`, etc.) - EZRA uses this to track which tasks are in-flight for which process
- Blocking pop holds the connection open and delivers a task the moment one arrives - no polling loop needed

Work distributes on demand: whichever worker finishes first asks for the next task and gets it immediately. Scale by running more workers - no coordination needed, no configuration changes in EZRA.

**A note on SQLite and remote access.** Nobody connects to SQLite remotely. Only EZRA's internal engine touches the file, on the same machine where EZRA runs. External clients talk to EZRA over TCP. The real constraint is that EZRA itself is single-node: all data lives on the one machine where it runs.

---

## Trade-offs

- **Single node.** All data lives on one machine. If that machine is unavailable, the queue is unavailable. The data itself is safe - SQLite is a plain file, easy to back up or replicate via any standard file-sync tool (rsync, litestream, filesystem snapshots). Uptime depends on the host, not EZRA.
- **At-least-once, not exactly-once.** A task can run more than once if the visibility timeout expires before the worker acks. This is a deliberate design choice - exactly-once delivery across a network is not something a queue can guarantee without distributed transaction coordination on both sides. Size your `visibility_timeout` correctly and design workers to handle duplicates.
- **Visibility timeout is not instant.** A crashed worker's tasks are reclaimed after `visibility_timeout` seconds, not immediately on disconnect. *(may be resolved in an upcoming release)*
- **Tasks accumulate.** Done tasks are kept forever unless `retention_seconds` is set on the queue. Without it the database grows without bound.
- **No fanout.** One task goes to exactly one worker. For broadcast patterns, push one task per subscriber.
- **No priority queues.** Workaround: separate queue names (`jobs.high`, `jobs.low`) with workers consuming both.
- **No delayed tasks.** Tasks are available immediately on push. Scheduled delivery is not supported. *(may be resolved in an upcoming release)*
- **100KB recommended payload limit.** SQLite handles larger BLOBs but performance degrades. Store large data externally and put a reference in the payload.
- **No cross-queue transactions.** Pushing to two queues atomically is not supported.

---

## Is EZRA right for you?

**Good fit**
- Background jobs: email delivery, PDF generation, image resizing, webhooks
- Any async work that needs reliability but not sub-millisecond latency
- Polyglot teams - each service uses its own language, all share one queue
- Early-stage products where running Kafka or RabbitMQ is disproportionate
- Single-machine or single-VM deployments where SQLite's single-node constraint is acceptable

**Poor fit**
- Multi-node high availability with no downtime window
- Pub/sub or fanout patterns where the same message must reach multiple consumers
- Throughput beyond SQLite's write ceiling (~30-80k/sec depending on disk)
- Event sourcing or audit logs where the stream itself is the primary data model
- Complex routing, filtering, or transformation at the broker level

---

## Install

> Prebuilt binaries: [github.com/entGriff/ezra/releases](https://github.com/entGriff/ezra/releases)
>
> Prefer containers? See [Docker in docs/usage.md](docs/usage.md#docker).

```bash
# macOS (Apple Silicon)
curl -Lo ezra https://github.com/entGriff/ezra/releases/latest/download/ezra-macos_arm64
chmod +x ezra

# Linux x86_64
curl -Lo ezra https://github.com/entGriff/ezra/releases/latest/download/ezra-linux_x86_64
chmod +x ezra

# Linux arm64
curl -Lo ezra https://github.com/entGriff/ezra/releases/latest/download/ezra-linux_arm64
chmod +x ezra
```

No runtime required. The binary is self-contained (~20 MB).

---

## Run

```bash
./ezra --data-dir /var/ezra
```

EZRA creates `ezra.db` in the data directory on first run. On every subsequent start it opens the existing file - your tasks are exactly where you left them.

Options can also be set via environment variables:

```bash
EZRA_DATA_DIR=/var/ezra EZRA_PORT=42002 ./ezra
```

Send `SIGTERM` or press `Ctrl+C` to stop. EZRA finishes any in-progress operations and shuts down cleanly.

For the full options reference, Docker deployment examples, language client snippets, and systemd setup see [docs/usage.md](docs/usage.md).

---

## Elixir

If you are building an Elixir application, EZRA can run embedded inside your own process - no TCP hop for your own workers.

```elixir
# mix.exs
{:ezra, "~> 0.1"}

# application.ex
children = [
  {Ezra, name: :ezra, data_dir: "priv/ezra"}
]
```

```elixir
# direct in-process call, no network
{:ok, id}   = Ezra.push(:ezra, "emails", payload)
{:ok, task} = Ezra.pop(:ezra, "emails", worker_id: "w1", block: 30_000)
:ok         = Ezra.ack(:ezra, task.id)
```

See [docs/elixir-client.md](docs/elixir-client.md) for the full guide.

---

## Terminology

**push** - add a new task to a queue.

**pop** - take the next task to work on. The task is not deleted - it is temporarily checked out. You must confirm when done.

**ack** (acknowledge) - tell EZRA "I finished this task." It is marked `done` and will not be given to anyone else. Done tasks stay in the database - they accumulate over time unless you configure `--retention-seconds` on the queue or push tasks with a `ttl_seconds` option.

**nack** (negative acknowledge) - tell EZRA "I failed." EZRA puts it back for another worker to try, up to `max_attempts` times.

**in_flight** - a task that has been popped but not yet acknowledged. If the worker goes silent, EZRA reclaims it after `visibility_timeout` seconds.

---

## Further reading

- [github.com/entGriff/ezra-examples](https://github.com/entGriff/ezra-examples/) - runnable Docker Compose demos (Python, Node.js)
- [docs/usage.md](docs/usage.md) - language clients, full usage examples, Docker, options reference, systemd
- [docs/architecture.md](docs/architecture.md) - storage schema, module map, wire protocol, telemetry
- [docs/elixir-client.md](docs/elixir-client.md) - Elixir library mode reference
