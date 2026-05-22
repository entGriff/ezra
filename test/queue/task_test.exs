defmodule Ezra.Queue.TaskTest do
  use ExUnit.Case, async: true

  alias Ezra.Queue.Task

  @now System.system_time(:microsecond)

  defp sample_row do
    # Must match Task.@columns order:
    # id, queue, payload, status, attempts, max_attempts,
    # inserted_at, scheduled_at, claimed_at, worker_id,
    # visibility_timeout, expires_at, last_error
    [1, "emails", ~s({"to":"alice@example.com"}), "available", 0, 3,
     @now, @now, nil, nil, 30, nil, nil]
  end

  describe "from_row/1" do
    test "builds a Task struct from a valid row" do
      task = Task.from_row(sample_row())

      assert task.id == 1
      assert task.queue == "emails"
      assert task.payload == ~s({"to":"alice@example.com"})
      assert task.status == "available"
      assert task.attempts == 0
      assert task.max_attempts == 3
      assert task.visibility_timeout == 30
      assert is_nil(task.claimed_at)
      assert is_nil(task.worker_id)
      assert is_nil(task.last_error)
    end

    test "maps nil columns correctly" do
      task = Task.from_row(sample_row())
      assert is_nil(task.expires_at)
    end
  end

  describe "valid_transition?/2" do
    test "available → in_flight is valid" do
      assert Task.valid_transition?(:available, :in_flight)
    end

    test "in_flight → done is valid" do
      assert Task.valid_transition?(:in_flight, :done)
    end

    test "in_flight → available is valid (requeue)" do
      assert Task.valid_transition?(:in_flight, :available)
    end

    test "in_flight → dead is valid (exhausted)" do
      assert Task.valid_transition?(:in_flight, :dead)
    end

    test "available → done is not valid" do
      refute Task.valid_transition?(:available, :done)
    end

    test "done → available is not valid" do
      refute Task.valid_transition?(:done, :available)
    end

    test "dead → available is not valid" do
      refute Task.valid_transition?(:dead, :available)
    end
  end

  describe "select_columns/0" do
    test "returns a non-empty SQL column list" do
      cols = Task.select_columns()
      assert is_binary(cols)
      assert String.contains?(cols, "id")
      assert String.contains?(cols, "queue")
      assert String.contains?(cols, "payload")
      assert String.contains?(cols, "status")
    end
  end
end
