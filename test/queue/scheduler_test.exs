defmodule Ezra.Queue.SchedulerTest do
  use Ezra.DBCase, async: true

  alias Ezra.Queue.{Engine, Scheduler}
  alias Ezra.Storage.SQLite

  # Start Engine + Scheduler with a very short sweep interval for tests.
  setup %{db: db} do
    uid = System.unique_integer([:positive])

    {:ok, engine} =
      start_supervised({Engine, db: db, name: :"sched_engine_#{uid}"}, id: :sched_engine)

    {:ok, scheduler} =
      start_supervised(
        {Scheduler, db: db, interval_ms: 50, name: :"scheduler_#{uid}"},
        id: :scheduler
      )

    {:ok, engine: engine, scheduler: scheduler}
  end

  describe "timeout reclaim" do
    test "in_flight task past visibility_timeout returns to available", %{engine: e, db: db} do
      # Push with a 0-second visibility timeout so it expires immediately
      Engine.push(e, "jobs", "task", visibility_timeout: 0)
      {:ok, task} = Engine.pop(e, "jobs", worker_id: "slow_worker")

      assert task.status == "in_flight"

      # Wait for scheduler to sweep (interval is 50ms, give it 3 cycles)
      Process.sleep(200)

      [[status]] = SQLite.query!(db, "SELECT status FROM tasks WHERE id = ?1", [task.id])
      assert status == "available"
    end

    test "attempts are not incremented by scheduler reclaim", %{engine: e, db: db} do
      # attempts is incremented by pop, not by the scheduler reclaim
      Engine.push(e, "jobs", "task", visibility_timeout: 0)
      {:ok, task} = Engine.pop(e, "jobs", worker_id: "slow_worker")

      Process.sleep(200)

      [[attempts]] = SQLite.query!(db, "SELECT attempts FROM tasks WHERE id = ?1", [task.id])
      # Still 1 from the original pop - scheduler does not increment
      assert attempts == 1
    end

    test "reclaimed task can be popped again", %{engine: e} do
      Engine.push(e, "retry", "task", visibility_timeout: 0)
      {:ok, _} = Engine.pop(e, "retry", worker_id: "w1")

      Process.sleep(200)

      assert {:ok, retried} = Engine.pop(e, "retry", worker_id: "w2")
      assert retried.payload == "task"
    end

    test "task with attempts >= max_attempts moves to dead, not available", %{engine: e, db: db} do
      Engine.push(e, "exhaust", "task", max_attempts: 1, visibility_timeout: 0)
      {:ok, task} = Engine.pop(e, "exhaust", worker_id: "w")

      assert task.attempts == 1

      Process.sleep(200)

      [[status]] = SQLite.query!(db, "SELECT status FROM tasks WHERE id = ?1", [task.id])
      assert status == "dead"
    end

    test "done tasks are not reclaimed", %{engine: e, db: db} do
      Engine.push(e, "done_test", "task")
      {:ok, task} = Engine.pop(e, "done_test", worker_id: "w")
      :ok = Engine.ack(e, task.id)

      Process.sleep(200)

      [[status]] = SQLite.query!(db, "SELECT status FROM tasks WHERE id = ?1", [task.id])
      assert status == "done"
    end
  end

  describe "TTL expiry" do
    test "expired task is deleted", %{engine: e, db: db} do
      # ttl_seconds: 0 means expires immediately (expires_at = now)
      Engine.push(e, "ephemeral", "vanish", ttl_seconds: 0)
      Process.sleep(200)

      rows = SQLite.query!(db, "SELECT id FROM tasks WHERE queue = ?1", ["ephemeral"])
      assert rows == []
    end

    test "non-expired task is kept", %{engine: e, db: db} do
      {:ok, id} = Engine.push(e, "keep", "stay", ttl_seconds: 3600)
      Process.sleep(200)

      rows = SQLite.query!(db, "SELECT id FROM tasks WHERE id = ?1", [id])
      assert rows != []
    end

    test "task without TTL is never deleted by scheduler", %{engine: e, db: db} do
      {:ok, id} = Engine.push(e, "permanent", "here_forever")
      Process.sleep(200)

      rows = SQLite.query!(db, "SELECT id FROM tasks WHERE id = ?1", [id])
      assert rows != []
    end
  end
end
