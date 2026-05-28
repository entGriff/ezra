defmodule Ezra.Queue.Scheduler do
  @moduledoc """
  Periodic sweep that maintains queue health.

  Runs every `interval` milliseconds (default 5 000) and performs two jobs:

  1. **Timeout reclaim** - any `in_flight` task whose `claimed_at +
     visibility_timeout` is in the past is moved back to `available` and its
     `attempts` counter is incremented.  If `attempts` reaches `max_attempts`
     the task is moved to `dead` instead.

  2. **TTL expiry** - any task with an `expires_at` timestamp in the past is
     hard-deleted regardless of status.
  """

  use GenServer
  require Logger

  alias Ezra.Storage.SQLite

  @default_interval_ms 5_000

  # --- Child spec / start ---

  def child_spec(opts) do
    %{
      id: Keyword.get(opts, :name, __MODULE__),
      start: {__MODULE__, :start_link, [opts]},
      type: :worker,
      restart: :permanent
    }
  end

  def start_link(opts) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  # --- GenServer callbacks ---

  @impl GenServer
  def init(opts) do
    db = Keyword.fetch!(opts, :db)
    interval = Keyword.get(opts, :interval_ms, @default_interval_ms)
    name = Keyword.get(opts, :name, __MODULE__)

    schedule_sweep(interval)
    {:ok, %{db: db, interval: interval, name: name}}
  end

  @impl GenServer
  def handle_info(:sweep, %{db: db, interval: interval, name: name} = state) do
    {reclaimed, dead_count} = reclaim_timed_out(db)
    expired = delete_expired(db)

    if reclaimed > 0 or dead_count > 0 or expired > 0 do
      Logger.debug("[Ezra.Scheduler] reclaimed=#{reclaimed} dead=#{dead_count} expired=#{expired}")
    end

    if reclaimed > 0 do
      :telemetry.execute([:ezra, :task, :timed_out], %{count: reclaimed}, %{engine: name})
    end

    if dead_count > 0 do
      :telemetry.execute([:ezra, :task, :dead], %{count: dead_count}, %{engine: name})
    end

    schedule_sweep(interval)
    {:noreply, state}
  end

  # --- Sweep logic ---

  defp reclaim_timed_out(db) do
    now = now_us()

    avail_rows = SQLite.query!(db, """
      UPDATE tasks
      SET    status     = 'available',
             claimed_at = NULL,
             worker_id  = NULL
      WHERE  status = 'in_flight'
        AND  (claimed_at + (visibility_timeout * 1000000)) < ?1
        AND  attempts < max_attempts
      RETURNING id
    """, [now])

    dead_rows = SQLite.query!(db, """
      UPDATE tasks
      SET    status = 'dead',
             claimed_at = NULL,
             worker_id  = NULL
      WHERE  status = 'in_flight'
        AND  (claimed_at + (visibility_timeout * 1000000)) < ?1
        AND  attempts >= max_attempts
      RETURNING id
    """, [now])

    {length(avail_rows), length(dead_rows)}
  end

  defp delete_expired(db) do
    now = now_us()

    rows = SQLite.query!(db, """
      DELETE FROM tasks
      WHERE  expires_at IS NOT NULL
        AND  expires_at < ?1
      RETURNING id
    """, [now])

    length(rows)
  end

  defp schedule_sweep(interval) do
    Process.send_after(self(), :sweep, interval)
  end

  defp now_us, do: System.system_time(:microsecond)
end
