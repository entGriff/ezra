defmodule Ezra.Queue.EngineTest do
  use Ezra.DBCase, async: true

  alias Ezra.Queue.Engine
  alias Ezra.Storage.SQLite

  # Start an Engine backed by the test's isolated SQLite connection.
  setup %{db: db} do
    {:ok, engine} = start_supervised({Engine, db: db, name: :"engine_#{System.unique_integer()}"})
    {:ok, engine: engine}
  end

  # ── push ───────────────────────────────────────────────────────────────────

  describe "push/4" do
    test "returns {:ok, id} with a positive integer id", %{engine: e} do
      assert {:ok, id} = Engine.push(e, "jobs", "payload")
      assert is_integer(id) and id > 0
    end

    test "creates the queue row on first push", %{engine: e, db: db} do
      Engine.push(e, "new_queue", "payload")

      rows = SQLite.query!(db, "SELECT name FROM queues WHERE name = ?1", ["new_queue"])
      assert rows == [["new_queue"]]
    end

    test "repeated pushes to the same queue don't duplicate the queue row", %{engine: e, db: db} do
      Engine.push(e, "emails", "p1")
      Engine.push(e, "emails", "p2")
      Engine.push(e, "emails", "p3")

      [[count]] = SQLite.query!(db, "SELECT COUNT(*) FROM queues WHERE name = ?1", ["emails"])
      assert count == 1
    end

    test "task is written with status 'available'", %{engine: e, db: db} do
      {:ok, id} = Engine.push(e, "jobs", "payload")

      [[status]] = SQLite.query!(db, "SELECT status FROM tasks WHERE id = ?1", [id])
      assert status == "available"
    end

    test "two different queues are independent", %{engine: e} do
      {:ok, id1} = Engine.push(e, "queue_a", "for_a")
      {:ok, id2} = Engine.push(e, "queue_b", "for_b")

      {:ok, t} = Engine.pop(e, "queue_a", worker_id: "w")
      assert t.id == id1

      {:ok, t} = Engine.pop(e, "queue_b", worker_id: "w")
      assert t.id == id2
    end
  end

  # ── pop ────────────────────────────────────────────────────────────────────

  describe "pop/3" do
    test "returns {:empty} on an empty queue", %{engine: e} do
      assert {:empty} = Engine.pop(e, "empty_queue")
    end

    test "returns {:ok, task} with correct fields", %{engine: e} do
      Engine.push(e, "jobs", ~s({"x":1}))
      assert {:ok, task} = Engine.pop(e, "jobs", worker_id: "worker-1")

      assert task.queue == "jobs"
      assert task.payload == ~s({"x":1})
      assert task.status == "in_flight"
      assert task.worker_id == "worker-1"
      assert task.attempts == 1
    end

    test "FIFO: tasks are returned oldest-first", %{engine: e} do
      {:ok, id1} = Engine.push(e, "fifo", "first")
      {:ok, id2} = Engine.push(e, "fifo", "second")
      {:ok, id3} = Engine.push(e, "fifo", "third")

      {:ok, t1} = Engine.pop(e, "fifo", worker_id: "w")
      {:ok, t2} = Engine.pop(e, "fifo", worker_id: "w")
      {:ok, t3} = Engine.pop(e, "fifo", worker_id: "w")

      assert t1.id == id1
      assert t2.id == id2
      assert t3.id == id3
    end

    test "a task claimed by one worker is invisible to another", %{engine: e} do
      Engine.push(e, "exclusive", "task")

      {:ok, _} = Engine.pop(e, "exclusive", worker_id: "worker-1")
      assert {:empty} = Engine.pop(e, "exclusive", worker_id: "worker-2")
    end

    test "blocking pop resolves when a task is pushed", %{engine: e} do
      parent = self()

      # Worker blocks in a separate process
      Task.start(fn ->
        result = Engine.pop(e, "async_q", worker_id: "w1", block: 2_000)
        send(parent, {:popped, result})
      end)

      # Small delay to ensure the worker is registered as a waiter
      Process.sleep(50)

      Engine.push(e, "async_q", "hello")

      assert_receive {:popped, {:ok, task}}, 1_000
      assert task.payload == "hello"
    end

    test "blocking pop returns {:empty} on timeout", %{engine: e} do
      t0 = System.monotonic_time(:millisecond)
      result = Engine.pop(e, "always_empty", worker_id: "w", block: 100)
      elapsed = System.monotonic_time(:millisecond) - t0

      assert result == {:empty}
      assert elapsed >= 100
    end
  end

  # ── ack ────────────────────────────────────────────────────────────────────

  describe "ack/2" do
    test "marks task as done", %{engine: e, db: db} do
      {:ok, id} = Engine.push(e, "jobs", "task")
      {:ok, _} = Engine.pop(e, "jobs", worker_id: "w")

      assert :ok = Engine.ack(e, id)

      [[status]] = SQLite.query!(db, "SELECT status FROM tasks WHERE id = ?1", [id])
      assert status == "done"
    end

    test "returns {:error, :not_found} for unknown id", %{engine: e} do
      assert {:error, :not_found} = Engine.ack(e, 999_999)
    end

    test "returns {:error, :not_found} for an available (not in_flight) task", %{engine: e} do
      {:ok, id} = Engine.push(e, "jobs", "task")
      assert {:error, :not_found} = Engine.ack(e, id)
    end

    test "acknowledged task is not retrievable by pop", %{engine: e} do
      {:ok, id} = Engine.push(e, "jobs", "task")
      {:ok, _} = Engine.pop(e, "jobs", worker_id: "w")
      :ok = Engine.ack(e, id)

      assert {:empty} = Engine.pop(e, "jobs", worker_id: "w2")
    end
  end

  # ── nack ───────────────────────────────────────────────────────────────────

  describe "nack/3" do
    test "requeues the task when attempts < max_attempts", %{engine: e, db: db} do
      Engine.push(e, "jobs", "task")
      {:ok, task} = Engine.pop(e, "jobs", worker_id: "w")

      assert {:ok, :requeued} = Engine.nack(e, task.id, reason: "timeout")

      [[status, error]] =
        SQLite.query!(db, "SELECT status, last_error FROM tasks WHERE id = ?1", [task.id])

      assert status == "available"
      assert error == "timeout"
    end

    test "requeued task can be popped again", %{engine: e} do
      {:ok, id} = Engine.push(e, "jobs", "task")
      {:ok, task} = Engine.pop(e, "jobs", worker_id: "w1")
      {:ok, :requeued} = Engine.nack(e, task.id)

      {:ok, retried} = Engine.pop(e, "jobs", worker_id: "w2")
      assert retried.id == id
      assert retried.attempts == 2
    end

    test "moves to dead when attempts reach max_attempts", %{engine: e, db: db} do
      Engine.push(e, "jobs", "task", max_attempts: 2)

      # First attempt
      {:ok, t1} = Engine.pop(e, "jobs", worker_id: "w")
      {:ok, :requeued} = Engine.nack(e, t1.id)

      # Second attempt (attempts will be 2 == max_attempts)
      {:ok, t2} = Engine.pop(e, "jobs", worker_id: "w")
      assert {:ok, :dead} = Engine.nack(e, t2.id, reason: "gave up")

      [[status]] = SQLite.query!(db, "SELECT status FROM tasks WHERE id = ?1", [t2.id])
      assert status == "dead"
    end

    test "dead task is visible via queue::dead", %{engine: e} do
      Engine.push(e, "jobs", "dying_task", max_attempts: 1)
      {:ok, task} = Engine.pop(e, "jobs", worker_id: "w")
      {:ok, :dead} = Engine.nack(e, task.id)

      assert {:ok, dead_task} = Engine.pop(e, "jobs::dead", worker_id: "monitor")
      assert dead_task.id == task.id
    end

    test "dead task does not reappear in main queue", %{engine: e} do
      Engine.push(e, "jobs", "dying_task", max_attempts: 1)
      {:ok, task} = Engine.pop(e, "jobs", worker_id: "w")
      {:ok, :dead} = Engine.nack(e, task.id)

      assert {:empty} = Engine.pop(e, "jobs", worker_id: "w2")
    end

    test "returns {:error, :not_found} for unknown id", %{engine: e} do
      assert {:error, :not_found} = Engine.nack(e, 999_999)
    end
  end

  # ── queue_info ─────────────────────────────────────────────────────────────

  describe "queue_info/2" do
    test "returns zero counts for an empty queue", %{engine: e} do
      Engine.push(e, "stats_q", "seed") # creates the queue
      {:ok, task} = Engine.pop(e, "stats_q", worker_id: "w")
      Engine.ack(e, task.id)

      info = Engine.queue_info(e, "stats_q")
      assert info.available == 0
      assert info.in_flight == 0
      assert info.dead == 0
    end

    test "counts available, in_flight and dead correctly", %{engine: e} do
      # Push 3 tasks
      Engine.push(e, "mixed", "t1")
      Engine.push(e, "mixed", "t2")
      Engine.push(e, "mixed", "t3", max_attempts: 1)

      # Pop 2
      {:ok, t1} = Engine.pop(e, "mixed", worker_id: "w")
      {:ok, _t2} = Engine.pop(e, "mixed", worker_id: "w")

      # Kill t1
      Engine.nack(e, t1.id)

      info = Engine.queue_info(e, "mixed")
      assert info.available == 2  # t1 requeued + t3 still available
      assert info.in_flight == 1  # t2
      assert info.dead == 0
    end
  end

  # ── persistence ────────────────────────────────────────────────────────────

  describe "persistence" do
    test "tasks survive engine restart", %{db_path: path} do
      uid = System.unique_integer([:positive])

      {:ok, db2} =
        start_supervised(
          {Ezra.Storage.SQLite, path: path, name: :"persist_db_#{uid}"},
          id: :pdb
        )

      {:ok, eng2} =
        start_supervised({Engine, db: db2, name: :"persist_engine_#{uid}"}, id: :peng)

      Engine.push(eng2, "durable", "important_task")

      :ok = stop_supervised(:peng)

      {:ok, eng3} =
        start_supervised({Engine, db: db2, name: :"persist_engine2_#{uid}"}, id: :peng2)

      assert {:ok, task} = Engine.pop(eng3, "durable", worker_id: "w")
      assert task.payload == "important_task"
    end
  end
end
