# Using EZRA from Elixir

EZRA can run embedded inside your own Elixir application - no TCP round-trip, no serialization overhead for your own workers. Add it to your supervision tree and call it as a local function.

This is entirely optional. If you are using EZRA from Python, Node, Go, or any other language, ignore this file and use the TCP path described in the main README.

---

## Installation

```elixir
# mix.exs
defp deps do
  [
    {:ezra, "~> 0.1"}
  ]
end
```

---

## Add to your supervision tree

```elixir
# lib/my_app/application.ex
def start(_type, _args) do
  children = [
    {Ezra,
      name: :ezra,
      data_dir: "priv/ezra",
      visibility_timeout: 60,
      max_attempts: 5}
  ]

  Supervisor.start_link(children, strategy: :one_for_one, name: MyApp.Supervisor)
end
```

Pass `port: 42002` if you also want external workers (Python, Node, etc.) to connect over TCP alongside your Elixir workers. Omit it if all workers are Elixir.

```elixir
# With TCP for mixed-language setups
{Ezra,
  name: :ezra,
  data_dir: "priv/ezra",
  port: 42002}
```

---

## Options

| Option | Default | Description |
|---|---|---|
| `name` | `Ezra` | Process name for this instance |
| `data_dir` | *(required)* | Directory where `ezra.db` is stored |
| `port` | `nil` (no TCP) | TCP port for RESP server; omit to disable |
| `host` | `"0.0.0.0"` | Bind address when `port:` is set |
| `visibility_timeout` | `30` | Seconds before an in-flight task is requeued |
| `max_attempts` | `3` | Total attempts before a task moves to dead |
| `retention_seconds` | `nil` (off) | Auto-delete tasks older than N seconds |
| `scheduler_ms` | `5000` | How often EZRA checks for timed-out tasks |

---

## Push a task

```elixir
payload = Jason.encode!(%{to: "alice@example.com", subject: "Welcome"})

{:ok, task_id} = Ezra.push(:ezra, "emails", payload)
```

The second argument is the queue name. Create as many queues as you need by using different names.

---

## Pop and process

```elixir
case Ezra.pop(:ezra, "emails", worker_id: "w1", block: 30_000) do
  {:ok, task} ->
    send_email(task.payload)
    :ok = Ezra.ack(:ezra, task.id)

  {:empty} ->
    # queue was empty for the full block duration - normal, just loop
    :ok
end
```

`block:` is how long in milliseconds to wait for a task if the queue is empty. The call returns immediately when a task arrives. Use `block: 0` to return immediately if no task is available.

---

## Worker loop

A simple GenServer pattern:

```elixir
defmodule MyApp.EmailWorker do
  use GenServer

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

  def init(opts) do
    worker_id = "email-worker-#{:erlang.unique_integer([:positive])}"
    send(self(), :pop)
    {:ok, Map.put(opts, :worker_id, worker_id)}
  end

  def handle_info(:pop, state) do
    case Ezra.pop(:ezra, "emails", worker_id: state.worker_id, block: 30_000) do
      {:ok, task} ->
        process(task)

      {:empty} ->
        :ok
    end

    send(self(), :pop)
    {:noreply, state}
  end

  defp process(task) do
    payload = Jason.decode!(task.payload)
    MyApp.Mailer.send(payload)
    Ezra.ack(:ezra, task.id)
  rescue
    e ->
      # Let visibility_timeout reclaim the task, or nack explicitly:
      Ezra.nack(:ezra, task.id, reason: Exception.message(e))
  end
end
```

Scale workers by adding more children to your supervisor:

```elixir
children = [
  {Ezra, name: :ezra, data_dir: "priv/ezra"},
  {MyApp.EmailWorker, []},
  {MyApp.EmailWorker, []},
  {MyApp.EmailWorker, []}
]
```

---

## Acknowledge vs nack

```elixir
# Task completed successfully
:ok = Ezra.ack(:ezra, task.id)

# Task failed - put it back for retry (up to max_attempts)
{:ok, _} = Ezra.nack(:ezra, task.id, reason: "connection refused")
```

If a worker process crashes without calling `ack` or `nack`, EZRA reclaims the task automatically after `visibility_timeout` seconds.

---

## Queue stats

```elixir
%{available: waiting, in_flight: processing, dead: failed} =
  Ezra.queue_info(:ezra, "emails")
```

---

## Dead-letter queue

Tasks that exhaust `max_attempts` move to `<queue>::dead`. Read them with a separate pop:

```elixir
{:ok, task} = Ezra.pop(:ezra, "emails::dead", worker_id: "inspector", block: 0)
IO.inspect(task.last_error)
```

---

## Multiple instances

You can run more than one EZRA instance in the same application - useful for isolating queue storage or exposing different TCP ports:

```elixir
children = [
  {Ezra, name: :work_queue, data_dir: "priv/work",    port: 42002},
  {Ezra, name: :audit_log,  data_dir: "priv/audit"}
]
```

Each instance has its own SQLite file and its own set of queues.
