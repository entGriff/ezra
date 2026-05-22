defmodule Ezra.Queue.Engine do
  @moduledoc """
  The central GenServer for one EZRA instance.

  All mutations (push, pop, ack, nack) go through this process, which
  is the single writer to the SQLite database. Reads that need coordination
  (blocking pop) also go through here; pure reads (stats) can bypass it.

  ## Blocking pop

  When `pop` is called with `block: ms` and the queue is empty, the Engine
  stores the caller (`GenServer.from`) in a waiters map and does NOT reply
  immediately. When the next push arrives for that queue, the Engine replies
  directly to the waiting caller, bypassing the queue entirely for that
  round-trip. If the timeout fires before a task arrives, the Engine replies
  `{:empty}`.

  ## State

    * `:db`      - pid of the `Ezra.Storage.SQLite` GenServer
    * `:waiters` - `%{queue_name => [{from, timer_ref}]}`
  """

  use GenServer
  require Logger

  alias Ezra.Queue.Task
  alias Ezra.Storage.SQLite


  # --- Child spec / start ---

  def child_spec(opts) do
    %{
      id: Keyword.get(opts, :name, __MODULE__),
      start: {__MODULE__, :start_link, [opts]},
      type: :worker,
      restart: :permanent,
      # Allow in-progress GenServer calls to finish on shutdown
      shutdown: 30_000
    }
  end

  def start_link(opts) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  # --- Public API ---

  @doc """
  Pushes a new task onto the named queue.

  Returns `{:ok, task_id}`.
  """
  @spec push(GenServer.server(), String.t(), binary(), keyword()) ::
          {:ok, pos_integer()}
  def push(server \\ __MODULE__, queue, payload, opts \\ []) do
    GenServer.call(server, {:push, queue, payload, opts})
  end

  @doc """
  Pops the oldest available task from the named queue.

  Options:
  - `worker_id` - identifies the consumer (default: `"unknown"`)
  - `block`     - milliseconds to wait if queue is empty (default: 0 = no wait)

  Returns `{:ok, task}` or `{:empty}`.
  """
  @spec pop(GenServer.server(), String.t(), keyword()) ::
          {:ok, Task.t()} | {:empty}
  def pop(server \\ __MODULE__, queue, opts \\ []) do
    timeout = Keyword.get(opts, :block, 0)
    call_timeout = if timeout > 0, do: timeout + 5_000, else: 5_000
    GenServer.call(server, {:pop, queue, opts}, call_timeout)
  end

  @doc """
  Acknowledges successful processing. Marks the task as `done`.

  Returns `:ok` or `{:error, :not_found}`.
  """
  @spec ack(GenServer.server(), pos_integer()) :: :ok | {:error, :not_found}
  def ack(server \\ __MODULE__, task_id) do
    GenServer.call(server, {:ack, task_id})
  end

  @doc """
  Negative-acknowledges a task, returning it to the queue or moving it to dead.

  - If `attempts < max_attempts` → status set back to `available`.
  - If `attempts >= max_attempts` → status set to `dead`.

  The `queue::dead` virtual queue is queryable via the same pop interface.

  Options:
  - `reason` - error message stored in `last_error` (default: nil)
  """
  @spec nack(GenServer.server(), pos_integer(), keyword()) ::
          {:ok, :requeued | :dead} | {:error, :not_found}
  def nack(server \\ __MODULE__, task_id, opts \\ []) do
    GenServer.call(server, {:nack, task_id, opts})
  end

  @doc """
  Returns stats for the named queue.
  """
  @spec queue_info(GenServer.server(), String.t()) :: map()
  def queue_info(server \\ __MODULE__, queue) do
    GenServer.call(server, {:queue_info, queue})
  end

  @doc """
  Ensures the named queue exists (creates it with defaults if not).
  Used by `XGROUP CREATE` to match Redis MKSTREAM semantics.
  """
  @spec ensure_queue(GenServer.server(), String.t()) :: :ok
  def ensure_queue(server \\ __MODULE__, queue) do
    GenServer.call(server, {:ensure_queue, queue})
  end

  @doc """
  Returns rich queue stats for the `XINFO STREAM` command.
  """
  @spec xinfo_stream(GenServer.server(), String.t()) :: map()
  def xinfo_stream(server \\ __MODULE__, queue) do
    GenServer.call(server, {:xinfo_stream, queue})
  end

  # --- GenServer callbacks ---

  @impl GenServer
  def init(opts) do
    db = Keyword.fetch!(opts, :db)
    name = Keyword.get(opts, :name, __MODULE__)
    defaults = %{
      visibility_timeout: Keyword.get(opts, :default_visibility_timeout, 30),
      max_attempts: Keyword.get(opts, :default_max_attempts, 3),
      retention_seconds: Keyword.get(opts, :default_retention_seconds, nil)
    }
    {:ok, %{db: db, waiters: %{}, name: name, defaults: defaults}}
  end

  @impl GenServer
  def handle_call({:push, queue, payload, opts}, _from, state) do
    opts = with_defaults(opts, state.defaults)
    create_queue_if_absent(state.db, queue, opts)
    task = do_insert(state.db, queue, payload, opts)
    :telemetry.execute([:ezra, :task, :pushed], %{count: 1}, %{queue: queue, task_id: task.id, engine: state.name})
    state = notify_waiter(state, queue, task)
    {:reply, {:ok, task.id}, state}
  end

  def handle_call({:pop, queue, opts}, from, state) do
    worker_id = Keyword.get(opts, :worker_id, "unknown")
    block_ms = Keyword.get(opts, :block, 0)

    case do_pop(state.db, queue, worker_id) do
      {:ok, task} ->
        :telemetry.execute([:ezra, :task, :popped], %{count: 1}, %{queue: queue, task_id: task.id, worker_id: worker_id, engine: state.name})
        {:reply, {:ok, task}, state}

      {:empty} when block_ms > 0 ->
        timer = Process.send_after(self(), {:pop_timeout, from}, block_ms)
        waiters = Map.update(state.waiters, queue, [{from, timer, worker_id}], &[{from, timer, worker_id} | &1])
        {:noreply, %{state | waiters: waiters}}

      {:empty} ->
        {:reply, {:empty}, state}
    end
  end

  def handle_call({:ack, task_id}, _from, state) do
    result = do_ack(state.db, task_id)
    if result == :ok do
      :telemetry.execute([:ezra, :task, :acked], %{count: 1}, %{task_id: task_id, engine: state.name})
    end
    {:reply, result, state}
  end

  def handle_call({:nack, task_id, opts}, _from, state) do
    result = do_nack(state.db, task_id, opts, state.name)
    {:reply, result, state}
  end

  def handle_call({:queue_info, queue}, _from, state) do
    info = do_queue_info(state.db, queue)
    {:reply, info, state}
  end

  def handle_call({:ensure_queue, queue}, _from, state) do
    create_queue_if_absent(state.db, queue, with_defaults([], state.defaults))
    {:reply, :ok, state}
  end

  def handle_call({:xinfo_stream, queue}, _from, state) do
    info = do_xinfo_stream(state.db, queue)
    {:reply, info, state}
  end

  @impl GenServer
  def handle_info({:pop_timeout, from}, state) do
    GenServer.reply(from, {:empty})
    # Remove this waiter from the map
    state = remove_waiter(state, from)
    {:noreply, state}
  end

  # --- Database operations ---

  defp create_queue_if_absent(db, queue, opts) do
    now = now_us()
    max_attempts = Keyword.fetch!(opts, :max_attempts)
    visibility_timeout = Keyword.fetch!(opts, :visibility_timeout)

    SQLite.query!(db, """
      INSERT INTO queues (name, max_attempts, visibility_timeout, created_at)
      VALUES (?1, ?2, ?3, ?4)
      ON CONFLICT (name) DO NOTHING
    """, [queue, max_attempts, visibility_timeout, now])
  end

  defp do_insert(db, queue, payload, opts) do
    now = now_us()
    max_attempts = Keyword.fetch!(opts, :max_attempts)
    visibility_timeout = Keyword.fetch!(opts, :visibility_timeout)
    scheduled_at = Keyword.get(opts, :scheduled_at, now)
    expires_at = case Keyword.get(opts, :ttl_seconds) || Keyword.get(opts, :retention_seconds) do
      nil -> nil
      seconds -> now + seconds * 1_000_000
    end

    rows = SQLite.query!(db, """
      INSERT INTO tasks
        (queue, payload, status, attempts, max_attempts, inserted_at,
         scheduled_at, visibility_timeout, expires_at)
      VALUES (?1, ?2, 'available', 0, ?3, ?4, ?5, ?6, ?7)
      RETURNING #{Task.select_columns()}
    """, [queue, payload, max_attempts, now, scheduled_at, visibility_timeout, expires_at])

    rows |> hd() |> Task.from_row()
  end

  defp do_pop(db, queue, worker_id) do
    now = now_us()
    # Resolve queue name for DLQ: "emails::dead" → queue="emails", status='dead'
    {base_queue, status_filter} = parse_queue_name(queue)

    rows = SQLite.query!(db, """
      UPDATE tasks
      SET    status    = 'in_flight',
             claimed_at = ?1,
             worker_id  = ?2,
             attempts   = attempts + 1
      WHERE id = (
        SELECT id FROM tasks
        WHERE  queue = ?3
          AND  status = ?4
          AND  scheduled_at <= ?5
        ORDER  BY scheduled_at ASC, id ASC
        LIMIT  1
      )
      RETURNING #{Task.select_columns()}
    """, [now, worker_id, base_queue, status_filter, now])

    case rows do
      [] -> {:empty}
      [row | _] -> {:ok, Task.from_row(row)}
    end
  end

  defp do_ack(db, task_id) do
    rows = SQLite.query!(db, """
      UPDATE tasks SET status = 'done'
      WHERE id = ?1 AND status = 'in_flight'
      RETURNING id
    """, [task_id])

    case rows do
      [] -> {:error, :not_found}
      _ -> :ok
    end
  end

  defp do_nack(db, task_id, opts, engine_name) do
    reason = Keyword.get(opts, :reason)

    rows = SQLite.query!(db, """
      SELECT id, queue, attempts, max_attempts FROM tasks
      WHERE id = ?1 AND status = 'in_flight'
    """, [task_id])

    case rows do
      [] ->
        {:error, :not_found}

      [[_id, queue, attempts, max_attempts]] ->
        new_status = if attempts >= max_attempts, do: "dead", else: "available"

        SQLite.query!(db, """
          UPDATE tasks
          SET status = ?1, last_error = ?2, claimed_at = NULL, worker_id = NULL
          WHERE id = ?3
        """, [new_status, reason, task_id])

        if new_status == "dead" do
          :telemetry.execute([:ezra, :task, :dead], %{count: 1}, %{task_id: task_id, queue: queue, engine: engine_name})
        end

        outcome = if new_status == "dead", do: :dead, else: :requeued
        {:ok, outcome}
    end
  end

  defp do_queue_info(db, queue) do
    {base_queue, _} = parse_queue_name(queue)

    [[available]] = SQLite.query!(db,
      "SELECT COUNT(*) FROM tasks WHERE queue = ?1 AND status = 'available'",
      [base_queue])

    [[in_flight]] = SQLite.query!(db,
      "SELECT COUNT(*) FROM tasks WHERE queue = ?1 AND status = 'in_flight'",
      [base_queue])

    [[dead]] = SQLite.query!(db,
      "SELECT COUNT(*) FROM tasks WHERE queue = ?1 AND status = 'dead'",
      [base_queue])

    %{queue: queue, available: available, in_flight: in_flight, dead: dead}
  end

  defp do_xinfo_stream(db, queue) do
    {base_queue, _} = parse_queue_name(queue)

    [[available]] = SQLite.query!(db,
      "SELECT COUNT(*) FROM tasks WHERE queue = ?1 AND status = 'available'", [base_queue])
    [[in_flight]] = SQLite.query!(db,
      "SELECT COUNT(*) FROM tasks WHERE queue = ?1 AND status = 'in_flight'", [base_queue])
    [[dead]] = SQLite.query!(db,
      "SELECT COUNT(*) FROM tasks WHERE queue = ?1 AND status = 'dead'", [base_queue])
    [[last_id]] = SQLite.query!(db,
      "SELECT MAX(id) FROM tasks WHERE queue = ?1", [base_queue])

    %{
      queue: queue,
      length: available + in_flight,
      available: available,
      in_flight: in_flight,
      dead: dead,
      last_id: last_id
    }
  end

  # --- Waiter dispatch ---

  defp notify_waiter(state, queue, task) do
    case Map.get(state.waiters, queue) do
      nil ->
        state

      [] ->
        %{state | waiters: Map.delete(state.waiters, queue)}

      [{from, timer, worker_id} | rest] ->
        Process.cancel_timer(timer)
        claimed = claim_for_waiter(state.db, task.id, worker_id)
        :telemetry.execute([:ezra, :task, :popped], %{count: 1}, %{queue: queue, task_id: claimed.id, worker_id: worker_id, engine: state.name})
        GenServer.reply(from, {:ok, claimed})
        %{state | waiters: Map.put(state.waiters, queue, rest)}
    end
  end

  defp claim_for_waiter(db, task_id, worker_id) do
    now = now_us()

    rows = SQLite.query!(db, """
      UPDATE tasks
      SET claimed_at = ?1, worker_id = ?2
      WHERE id = ?3
      RETURNING #{Task.select_columns()}
    """, [now, worker_id, task_id])

    rows |> hd() |> Task.from_row()
  end

  defp remove_waiter(state, target_from) do
    updated =
      Map.new(state.waiters, fn {queue, waiters} ->
        {queue, Enum.reject(waiters, fn {from, _timer, _wid} -> from == target_from end)}
      end)

    %{state | waiters: updated}
  end

  # --- Helpers ---

  defp with_defaults(opts, defaults) do
    Keyword.merge(
      [
        visibility_timeout: defaults.visibility_timeout,
        max_attempts: defaults.max_attempts
      ],
      opts
    )
  end

  # "emails::dead" → {"emails", "dead"}
  # "emails"       → {"emails", "available"}
  defp parse_queue_name(name) do
    case String.split(name, "::", parts: 2) do
      [base, "dead"] -> {base, "dead"}
      [base] -> {base, "available"}
      _ -> {name, "available"}
    end
  end

  defp now_us, do: System.system_time(:microsecond)
end
