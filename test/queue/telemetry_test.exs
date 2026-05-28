defmodule Ezra.Queue.TelemetryTest do
  use ExUnit.Case, async: true

  alias Ezra.Queue.{Engine, Scheduler}
  alias Ezra.Storage.SQLite

  # Each test gets its own SQLite + Engine to avoid event crosstalk.
  # db_name is exposed so nested setups can attach a Scheduler to the same db.
  setup do
    uid = System.unique_integer([:positive])
    path = "/tmp/ezra_telemetry_#{uid}.db"
    db_name = :"telemetry_db_#{uid}"
    eng_name = :"telemetry_engine_#{uid}"

    start_supervised!({SQLite, path: path, name: db_name})
    start_supervised!({Engine, db: db_name, name: eng_name})

    on_exit(fn -> File.rm_rf!(path) end)

    %{engine: eng_name, db: db_name}
  end

  # Attach a telemetry handler that only forwards events from the specific
  # engine under test - prevents cross-test contamination when tests run
  # concurrently and all share the global telemetry dispatcher.
  defp attach(test_pid, engine_name, events) do
    handler_id = "test-#{System.unique_integer()}"

    :telemetry.attach_many(
      handler_id,
      events,
      fn event, measurements, metadata, _ ->
        if metadata[:engine] == engine_name do
          send(test_pid, {:telemetry, event, measurements, metadata})
        end
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)
    handler_id
  end

  # ---------------------------------------------------------------------------

  test "push emits [:ezra, :task, :pushed]", %{engine: engine} do
    attach(self(), engine, [[:ezra, :task, :pushed]])

    {:ok, id} = Engine.push(engine, "emails", "hello")

    assert_receive {:telemetry, [:ezra, :task, :pushed], %{count: 1}, meta}
    assert meta.queue == "emails"
    assert meta.task_id == id
  end

  test "pop emits [:ezra, :task, :popped]", %{engine: engine} do
    attach(self(), engine, [[:ezra, :task, :popped]])

    {:ok, id} = Engine.push(engine, "emails", "hello")
    {:ok, task} = Engine.pop(engine, "emails", worker_id: "w1")

    assert task.id == id
    assert_receive {:telemetry, [:ezra, :task, :popped], %{count: 1}, meta}
    assert meta.queue == "emails"
    assert meta.task_id == id
    assert meta.worker_id == "w1"
  end

  test "blocking pop (waiter path) emits [:ezra, :task, :popped]", %{engine: engine} do
    attach(self(), engine, [[:ezra, :task, :popped]])

    # Start a blocking pop before any task exists
    waiter = Task.async(fn ->
      Engine.pop(engine, "bq", worker_id: "w1", block: 3_000)
    end)

    Process.sleep(30)

    {:ok, id} = Engine.push(engine, "bq", "data")
    {:ok, task} = Task.await(waiter, 4_000)

    assert task.id == id
    assert_receive {:telemetry, [:ezra, :task, :popped], %{count: 1}, meta}
    assert meta.queue == "bq"
    assert meta.worker_id == "w1"
  end

  test "ack emits [:ezra, :task, :acked]", %{engine: engine} do
    attach(self(), engine, [[:ezra, :task, :acked]])

    {:ok, id} = Engine.push(engine, "emails", "hello")
    {:ok, task} = Engine.pop(engine, "emails", worker_id: "w1")
    :ok = Engine.ack(engine, task.id)

    assert_receive {:telemetry, [:ezra, :task, :acked], %{count: 1}, meta}
    assert meta.task_id == id
  end

  test "nack beyond max_attempts emits [:ezra, :task, :dead]", %{engine: engine} do
    attach(self(), engine, [[:ezra, :task, :dead]])

    {:ok, id} = Engine.push(engine, "emails", "hello", max_attempts: 1)
    {:ok, _} = Engine.pop(engine, "emails", worker_id: "w1")
    {:ok, :dead} = Engine.nack(engine, id)

    assert_receive {:telemetry, [:ezra, :task, :dead], %{count: 1}, meta}
    assert meta.task_id == id
    assert meta.queue == "emails"
  end

  test "nack with retries remaining does NOT emit :dead", %{engine: engine} do
    attach(self(), engine, [[:ezra, :task, :dead]])

    {:ok, id} = Engine.push(engine, "emails", "hello", max_attempts: 3)
    {:ok, _} = Engine.pop(engine, "emails", worker_id: "w1")
    {:ok, :requeued} = Engine.nack(engine, id)

    refute_receive {:telemetry, [:ezra, :task, :dead], _, _}, 50
  end

  test "ack on unknown task does NOT emit :acked", %{engine: engine} do
    attach(self(), engine, [[:ezra, :task, :acked]])

    {:error, :not_found} = Engine.ack(engine, 999_999)

    refute_receive {:telemetry, [:ezra, :task, :acked], _, _}, 50
  end

  # ---------------------------------------------------------------------------


  describe "scheduler telemetry" do
    # Attach a scheduler to the db that the module-level setup already created.
    # Starting a second SQLite+Engine here would conflict with the module setup
    # since both setups run for each test in this describe block.
    setup %{db: db} do
      sched_name = :"telemetry_scheduler_#{System.unique_integer([:positive])}"
      start_supervised!({Scheduler, db: db, interval_ms: 50, name: sched_name})
      %{scheduler: sched_name}
    end

    test "timed-out task emits [:ezra, :task, :timed_out]", %{engine: engine, scheduler: scheduler} do
      attach(self(), scheduler, [[:ezra, :task, :timed_out]])

      Engine.push(engine, "timeout_q", "task", visibility_timeout: 0)
      {:ok, _} = Engine.pop(engine, "timeout_q", worker_id: "w")

      assert_receive {:telemetry, [:ezra, :task, :timed_out], %{count: n}, _}, 500
      assert n >= 1
    end
  end
end
