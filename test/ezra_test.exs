defmodule EzraTest do
  use ExUnit.Case, async: true

  # Each test gets its own Ezra instance so tests are fully isolated.
  setup do
    uid = System.unique_integer([:positive])
    name = :"test_ezra_#{uid}"
    data_dir = "/tmp/ezra_api_#{uid}"

    start_supervised!({Ezra, name: name, data_dir: data_dir})

    on_exit(fn -> File.rm_rf!(data_dir) end)

    %{name: name}
  end

  # ---------------------------------------------------------------------------
  # push/4
  # ---------------------------------------------------------------------------

  describe "push/4" do
    test "returns {:ok, id} with a positive integer id", %{name: n} do
      assert {:ok, id} = Ezra.push(n, "emails", "payload")
      assert is_integer(id) and id > 0
    end

    test "two pushes to the same queue return distinct ids", %{name: n} do
      {:ok, id1} = Ezra.push(n, "jobs", "a")
      {:ok, id2} = Ezra.push(n, "jobs", "b")
      assert id1 != id2
    end
  end

  # ---------------------------------------------------------------------------
  # pop/3
  # ---------------------------------------------------------------------------

  describe "pop/3" do
    test "returns {:empty} for a queue that has never received a task", %{name: n} do
      assert {:empty} = Ezra.pop(n, "never_pushed", worker_id: "w")
    end

    test "returns {:ok, task} with correct fields after a push", %{name: n} do
      Ezra.push(n, "emails", ~s({"to":"alice@example.com"}))
      assert {:ok, task} = Ezra.pop(n, "emails", worker_id: "worker-1")

      assert task.queue == "emails"
      assert task.payload == ~s({"to":"alice@example.com"})
      assert task.status == "in_flight"
      assert task.worker_id == "worker-1"
      assert task.attempts == 1
    end

    test "blocking pop resolves when a task is pushed", %{name: n} do
      worker = Task.async(fn ->
        Ezra.pop(n, "blocking", worker_id: "w1", block: 2_000)
      end)

      Process.sleep(100)

      Ezra.push(n, "blocking", "wake")

      assert {:ok, task} = Task.await(worker, 3_000)
      assert task.payload == "wake"
    end
  end

  # ---------------------------------------------------------------------------
  # ack/2
  # ---------------------------------------------------------------------------

  describe "ack/2" do
    test "returns :ok for an in-flight task", %{name: n} do
      Ezra.push(n, "jobs", "task")
      {:ok, task} = Ezra.pop(n, "jobs", worker_id: "w")
      assert :ok = Ezra.ack(n, task.id)
    end

    test "returns {:error, :not_found} for an unknown id", %{name: n} do
      assert {:error, :not_found} = Ezra.ack(n, 999_999)
    end

    test "acked task is not retrievable by a subsequent pop", %{name: n} do
      Ezra.push(n, "jobs", "task")
      {:ok, task} = Ezra.pop(n, "jobs", worker_id: "w1")
      :ok = Ezra.ack(n, task.id)

      assert {:empty} = Ezra.pop(n, "jobs", worker_id: "w2")
    end
  end

  # ---------------------------------------------------------------------------
  # nack/3
  # ---------------------------------------------------------------------------

  describe "nack/3" do
    test "returns {:ok, :requeued} when retries remain", %{name: n} do
      Ezra.push(n, "jobs", "task")
      {:ok, task} = Ezra.pop(n, "jobs", worker_id: "w")
      assert {:ok, :requeued} = Ezra.nack(n, task.id, reason: "oops")
    end

    test "requeued task can be popped again with incremented attempts", %{name: n} do
      {:ok, id} = Ezra.push(n, "jobs", "task")
      {:ok, task} = Ezra.pop(n, "jobs", worker_id: "w1")
      {:ok, :requeued} = Ezra.nack(n, task.id)

      {:ok, retried} = Ezra.pop(n, "jobs", worker_id: "w2")
      assert retried.id == id
      assert retried.attempts == 2
    end

    test "returns {:ok, :dead} when max_attempts is reached", %{name: n} do
      Ezra.push(n, "jobs", "task", max_attempts: 1)
      {:ok, task} = Ezra.pop(n, "jobs", worker_id: "w")
      assert {:ok, :dead} = Ezra.nack(n, task.id)
    end

    test "dead task appears in queue::dead", %{name: n} do
      Ezra.push(n, "jobs", "dying", max_attempts: 1)
      {:ok, task} = Ezra.pop(n, "jobs", worker_id: "w")
      {:ok, :dead} = Ezra.nack(n, task.id)

      assert {:ok, dead_task} = Ezra.pop(n, "jobs::dead", worker_id: "monitor")
      assert dead_task.id == task.id
    end

    test "returns {:error, :not_found} for an unknown id", %{name: n} do
      assert {:error, :not_found} = Ezra.nack(n, 999_999)
    end
  end

  # ---------------------------------------------------------------------------
  # queue_info/2
  # ---------------------------------------------------------------------------

  describe "queue_info/2" do
    test "counts reflect the current state of tasks in the queue", %{name: n} do
      Ezra.push(n, "mixed", "t1")
      Ezra.push(n, "mixed", "t2")
      Ezra.push(n, "mixed", "t3", max_attempts: 1)

      {:ok, t1} = Ezra.pop(n, "mixed", worker_id: "w")
      {:ok, _t2} = Ezra.pop(n, "mixed", worker_id: "w")

      # nack t1 - goes back to available (default max_attempts > 1)
      Ezra.nack(n, t1.id)

      info = Ezra.queue_info(n, "mixed")
      assert info.available == 2  # t1 requeued + t3 still waiting
      assert info.in_flight == 1  # t2
      assert info.dead == 0
    end

    test "returns zero counts for a queue that has never been used", %{name: n} do
      info = Ezra.queue_info(n, "never_used")
      assert info.available == 0
      assert info.in_flight == 0
      assert info.dead == 0
    end
  end

  # ---------------------------------------------------------------------------
  # Multiple instances
  # ---------------------------------------------------------------------------

  describe "multiple instances" do
    test "two instances do not share task data", %{name: inst_a} do
      uid = System.unique_integer([:positive])
      inst_b = :"test_ezra_b_#{uid}"
      dir_b = "/tmp/ezra_api_b_#{uid}"

      start_supervised!({Ezra, name: inst_b, data_dir: dir_b}, id: :inst_b)
      on_exit(fn -> File.rm_rf!(dir_b) end)

      Ezra.push(inst_a, "shared", "for_a")
      Ezra.push(inst_b, "shared", "for_b")

      {:ok, task_a} = Ezra.pop(inst_a, "shared", worker_id: "w")
      {:ok, task_b} = Ezra.pop(inst_b, "shared", worker_id: "w")

      assert task_a.payload == "for_a"
      assert task_b.payload == "for_b"

      # Verify there is no bleed: each queue is empty after its one task is popped
      assert {:empty} = Ezra.pop(inst_a, "shared", worker_id: "w")
      assert {:empty} = Ezra.pop(inst_b, "shared", worker_id: "w")
    end
  end
end
