# EZRA - Usage Reference

Client connection examples, full usage guide, Docker deployment, and binary configuration reference.

---

## Contents

- [Connect](#connect)
- [Push a task](#push-a-task)
- [Pop and process](#pop-and-process)
- [Continuous worker loop](#continuous-worker-loop)
- [Multiple queues](#multiple-queues)
- [Queue stats](#queue-stats)
- [Dead-letter queue](#dead-letter-queue)
- [Docker](#docker)
- [Binary options](#binary-options)
- [With systemd](#with-systemd)

---

## Connect

Use the Redis client your language already has - just point it at EZRA's port.

**Python**

```python
import redis
r = redis.Redis(host="localhost", port=42002, decode_responses=True)
```

**Node.js**

```js
import { createClient } from "redis"
const r = await createClient({ url: "redis://localhost:42002" }).connect()
```

**Go**

```go
rdb := redis.NewClient(&redis.Options{Addr: "localhost:42002"})
```

**Ruby**

```ruby
r = Redis.new(host: "localhost", port: 42002)
```

---

## Push a task

```python
import json, redis
r = redis.Redis(host="localhost", port=42002, decode_responses=True)

task_id = r.xadd("emails", {"payload": json.dumps({"to": "alice@example.com"})})
```

---

## Pop and process

`xreadgroup` takes the next available task and claims it. The `BLOCK` parameter tells EZRA how long to wait if the queue is empty - the connection stays open and EZRA delivers the task the moment one arrives. No polling.

```python
results = r.xreadgroup("workers", "worker-1", {"emails": ">"}, count=1, block=30_000)

if results:
    stream, entries = results[0]
    msg_id, fields = entries[0]

    send_email(json.loads(fields["payload"]))
    r.xack("emails", "workers", msg_id)
```

To report failure explicitly instead of waiting for the visibility timeout to reclaim the task:

```python
r.xnack("emails", "workers", msg_id)
```

EZRA returns the task to `available` immediately and increments its attempt counter. Once `max_attempts` is reached, it moves to the dead-letter queue instead.

---

## Continuous worker loop

Use `BLOCK 0` to wait indefinitely. The worker stays connected and receives tasks as they arrive.

```python
import json, redis

r = redis.Redis(host="localhost", port=42002, decode_responses=True)

while True:
    results = r.xreadgroup("workers", "worker-1", {"emails": ">"}, count=1, block=0)
    if results:
        stream, entries = results[0]
        msg_id, fields = entries[0]

        try:
            send_email(json.loads(fields["payload"]))
            r.xack("emails", "workers", msg_id)
        except Exception:
            pass  # let visibility_timeout reclaim and retry
```

Each worker process should use a distinct name (`worker-1`, `worker-2`, etc.). EZRA uses this to track which tasks are in-flight for which worker.

---

## Multiple queues

Each queue is independent. Create as many as you need by using different names - no configuration required.

```python
r.xadd("emails",   {"payload": "..."})
r.xadd("invoices", {"payload": "..."})
r.xadd("webhooks", {"payload": "..."})
```

---

## Queue stats

```python
r.xlen("emails")   # count of available (waiting) tasks
```

For a full picture across all queues, connect any SQLite client directly to `ezra.db`:

```sql
SELECT
    queue,
    COUNT(*) FILTER (WHERE status = 'available')  AS waiting,
    COUNT(*) FILTER (WHERE status = 'in_flight')  AS processing,
    COUNT(*) FILTER (WHERE status = 'done')        AS done,
    COUNT(*) FILTER (WHERE status = 'dead')        AS failed,
    COUNT(*)                                        AS total
FROM tasks
GROUP BY queue
ORDER BY queue;
```

---

## Dead-letter queue

Tasks that exhaust their attempt limit move to `<queue>::dead`. Read from it the same way:

```python
dead = r.xreadgroup("monitor", "inspector", {"emails::dead": ">"}, count=10)
```

---

## Docker

The official image works on both `linux/amd64` and `linux/arm64` - Docker pulls the right one automatically.

```bash
# Start with a persistent volume
docker run -d \
  --name ezra \
  -p 42002:42002 \
  -v ezra_data:/data \
  ghcr.io/entgriff/ezra

# Use a different host port (EZRA still runs on 42002 internally)
docker run -d \
  --name ezra \
  -p 9000:42002 \
  -v ezra_data:/data \
  ghcr.io/entgriff/ezra

# Override any option via env vars
docker run -d \
  --name ezra \
  -p 42002:42002 \
  -v ezra_data:/data \
  -e EZRA_VISIBILITY_TIMEOUT=60 \
  -e EZRA_MAX_ATTEMPTS=5 \
  ghcr.io/entgriff/ezra
```

```bash
docker stop ezra     # graceful shutdown
docker start ezra    # resume - all tasks are preserved
docker rm -f ezra    # destroy container (data volume survives)
```

---

## Binary options

All flags can also be set via environment variables. Environment variables take effect when the flag is not provided.

| Flag                     | Env variable              | Default      | Description                               |
| ------------------------ | ------------------------- | ------------ | ----------------------------------------- |
| `--data-dir PATH`        | `EZRA_DATA_DIR`           | *(required)* | Directory for `ezra.db`                   |
| `--port N`               | `EZRA_PORT`               | `42002`      | TCP port                                  |
| `--host ADDR`            | `EZRA_HOST`               | `0.0.0.0`    | Bind address                              |
| `--visibility-timeout N` | `EZRA_VISIBILITY_TIMEOUT` | `30`         | Seconds before a stuck task is requeued   |
| `--max-attempts N`       | `EZRA_MAX_ATTEMPTS`       | `3`          | Total attempts before a task is moved to dead |
| `--retention-seconds N`  | `EZRA_RETENTION_SECONDS`  | off          | Auto-delete tasks older than N seconds    |
| `--scheduler-ms N`       | `EZRA_SCHEDULER_MS`       | `5000`       | How often EZRA checks for timed-out tasks |

---

## With systemd

```ini
# /etc/systemd/system/ezra.service
[Unit]
Description=EZRA task queue
After=network.target

[Service]
ExecStart=/usr/local/bin/ezra --data-dir /var/ezra
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
```

```bash
systemctl start ezra
systemctl stop ezra
systemctl restart ezra
```
