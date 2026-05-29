defmodule Ezra.Server.ConnectionTest do
  use ExUnit.Case, async: true

  alias Ezra.Server.RESP

  # ---------------------------------------------------------------------------
  # Setup - unique Ezra instance per test, raw TCP client
  # ---------------------------------------------------------------------------

  setup do
    port = free_port()
    uid = System.unique_integer([:positive])
    name = :"ezra_conn_test_#{uid}"
    data_dir = "/tmp/ezra_conn_#{uid}"

    start_supervised!({Ezra, name: name, data_dir: data_dir, port: port})

    socket = tcp_connect(port)

    on_exit(fn ->
      :gen_tcp.close(socket)
      File.rm_rf!(data_dir)
    end)

    %{socket: socket, port: port, data_dir: data_dir}
  end

  # ---------------------------------------------------------------------------
  # XADD
  # ---------------------------------------------------------------------------

  test "XADD returns bulk string task id", %{socket: socket} do
    id = send_command!(socket, ["XADD", "emails", "*", "payload", "hello"])
    assert is_binary(id)
    assert String.match?(id, ~r/^\d+$/)
  end

  test "XADD accepts optional fields", %{socket: socket} do
    id = send_command!(socket, ["XADD", "jobs", "*", "payload", "data", "max_attempts", "5"])
    assert is_binary(id)
  end

  # ---------------------------------------------------------------------------
  # XGROUP CREATE
  # ---------------------------------------------------------------------------

  test "XGROUP CREATE returns OK", %{socket: socket} do
    resp = send_command!(socket, ["XGROUP", "CREATE", "emails", "workers", "$", "MKSTREAM"])
    assert {:simple, "OK"} = resp
  end

  # ---------------------------------------------------------------------------
  # XREADGROUP (non-blocking)
  # ---------------------------------------------------------------------------

  test "XREADGROUP on empty queue returns stream with empty entries", %{socket: socket} do
    result = send_command!(socket, ["XREADGROUP", "GROUP", "workers", "w1",
                                     "COUNT", "1", "STREAMS", "emails", ">"])
    assert [["emails", []]] = result
  end

  test "XREADGROUP returns task after XADD", %{socket: socket} do
    id = send_command!(socket, ["XADD", "emails", "*", "payload", "hello world"])
    result = send_command!(socket, ["XREADGROUP", "GROUP", "workers", "w1",
                                     "COUNT", "1", "STREAMS", "emails", ">"])

    assert [["emails", [[^id, fields]]]] = result
    assert field_value(fields, "payload") == "hello world"
  end

  test "XREADGROUP is FIFO - tasks arrive in insertion order", %{socket: socket} do
    id1 = send_command!(socket, ["XADD", "fifo", "*", "payload", "first"])
    id2 = send_command!(socket, ["XADD", "fifo", "*", "payload", "second"])

    r1 = send_command!(socket, ["XREADGROUP", "GROUP", "workers", "w1",
                                 "COUNT", "1", "STREAMS", "fifo", ">"])
    r2 = send_command!(socket, ["XREADGROUP", "GROUP", "workers", "w1",
                                 "COUNT", "1", "STREAMS", "fifo", ">"])

    assert [["fifo", [[^id1, _]]]] = r1
    assert [["fifo", [[^id2, _]]]] = r2
  end

  # ---------------------------------------------------------------------------
  # XACK
  # ---------------------------------------------------------------------------

  test "XACK after pop returns 1", %{socket: socket} do
    id = send_command!(socket, ["XADD", "emails", "*", "payload", "msg"])
    send_command!(socket, ["XREADGROUP", "GROUP", "workers", "w1",
                            "COUNT", "1", "STREAMS", "emails", ">"])
    n = send_command!(socket, ["XACK", "emails", "workers", id])
    assert n == 1
  end

  test "XACK on unknown id returns 0", %{socket: socket} do
    n = send_command!(socket, ["XACK", "emails", "workers", "9999999"])
    assert n == 0
  end

  test "full XADD → XREADGROUP → XACK roundtrip", %{socket: socket} do
    id = send_command!(socket, ["XADD", "emails", "*", "payload", "roundtrip"])

    [["emails", [[^id, fields]]]] =
      send_command!(socket, ["XREADGROUP", "GROUP", "workers", "w1",
                              "COUNT", "1", "STREAMS", "emails", ">"])

    assert field_value(fields, "payload") == "roundtrip"

    assert 1 = send_command!(socket, ["XACK", "emails", "workers", id])
  end

  # ---------------------------------------------------------------------------
  # CLIENT SETNAME
  # ---------------------------------------------------------------------------

  test "CLIENT SETNAME returns OK", %{socket: socket} do
    resp = send_command!(socket, ["CLIENT", "SETNAME", "my-worker"])
    assert {:simple, "OK"} = resp
  end

  test "connection stays healthy after CLIENT SETNAME", %{socket: socket} do
    send_command!(socket, ["CLIENT", "SETNAME", "my-worker"])
    id = send_command!(socket, ["XADD", "q", "*", "payload", "after_setname"])
    assert is_binary(id)
  end

  # ---------------------------------------------------------------------------
  # XDEL (nack)
  # ---------------------------------------------------------------------------

  test "XDEL on in-flight task returns 1", %{socket: socket} do
    id = send_command!(socket, ["XADD", "emails", "*", "payload", "msg"])
    send_command!(socket, ["XREADGROUP", "GROUP", "workers", "w1",
                            "COUNT", "1", "STREAMS", "emails", ">"])
    n = send_command!(socket, ["XDEL", "emails", id])
    assert n == 1
  end

  test "XDEL on unknown id returns 0", %{socket: socket} do
    n = send_command!(socket, ["XDEL", "emails", "9999999"])
    assert n == 0
  end

  test "XDEL returns task to available - can be popped again", %{socket: socket} do
    id = send_command!(socket, ["XADD", "emails", "*", "payload", "retry-me"])

    send_command!(socket, ["XREADGROUP", "GROUP", "workers", "w1",
                            "COUNT", "1", "STREAMS", "emails", ">"])

    send_command!(socket, ["XDEL", "emails", id])

    result = send_command!(socket, ["XREADGROUP", "GROUP", "workers", "w1",
                                    "COUNT", "1", "STREAMS", "emails", ">"])
    assert [["emails", [[^id, fields]]]] = result
    assert field_value(fields, "payload") == "retry-me"
  end

  # ---------------------------------------------------------------------------
  # XNACK
  # ---------------------------------------------------------------------------

  test "XNACK on in-flight task returns OK", %{socket: socket} do
    id = send_command!(socket, ["XADD", "emails", "*", "payload", "msg"])
    send_command!(socket, ["XREADGROUP", "GROUP", "workers", "w1",
                            "COUNT", "1", "STREAMS", "emails", ">"])
    resp = send_command!(socket, ["XNACK", "emails", "workers", id])
    assert {:simple, "OK"} = resp
  end

  test "XNACK on unknown id returns error", %{socket: socket} do
    resp = send_command!(socket, ["XNACK", "emails", "workers", "9999999"])
    assert {:error, _} = resp
  end

  # ---------------------------------------------------------------------------
  # XLEN
  # ---------------------------------------------------------------------------

  test "XLEN returns available task count", %{socket: socket} do
    assert 0 = send_command!(socket, ["XLEN", "emails"])
    send_command!(socket, ["XADD", "emails", "*", "payload", "a"])
    send_command!(socket, ["XADD", "emails", "*", "payload", "b"])
    assert 2 = send_command!(socket, ["XLEN", "emails"])
  end

  test "XLEN does not count in-flight tasks", %{socket: socket} do
    send_command!(socket, ["XADD", "emails", "*", "payload", "msg"])
    send_command!(socket, ["XREADGROUP", "GROUP", "workers", "w1",
                            "COUNT", "1", "STREAMS", "emails", ">"])
    assert 0 = send_command!(socket, ["XLEN", "emails"])
  end

  # ---------------------------------------------------------------------------
  # XINFO STREAM
  # ---------------------------------------------------------------------------

  test "XINFO STREAM returns stats", %{socket: socket} do
    send_command!(socket, ["XADD", "emails", "*", "payload", "x"])
    result = send_command!(socket, ["XINFO", "STREAM", "emails"])
    assert is_list(result)
    assert "emails" == field_value(result, "name")
    assert 1 == field_value(result, "length")
  end

  # ---------------------------------------------------------------------------
  # Unknown command
  # ---------------------------------------------------------------------------

  test "unknown command returns error", %{socket: socket} do
    resp = send_command!(socket, ["PING"])
    assert {:error, msg} = resp
    assert String.contains?(msg, "ping")
  end

  test "connection survives unknown command", %{socket: socket} do
    send_command!(socket, ["PING"])
    id = send_command!(socket, ["XADD", "q", "*", "payload", "ok"])
    assert is_binary(id)
  end

  # ---------------------------------------------------------------------------
  # Pipelining - multiple commands in one TCP write
  # ---------------------------------------------------------------------------

  test "handles pipelined commands in a single TCP packet", %{socket: socket} do
    # Send XADD + XLEN back-to-back without reading between.
    # Both responses may arrive in the same TCP segment (common on loopback /
    # Alpine musl), so we use recv_n which threads the leftover buffer through
    # successive decodes instead of discarding it.
    cmd1 = IO.iodata_to_binary(RESP.encode(["XADD", "pipe", "*", "payload", "hi"]))
    cmd2 = IO.iodata_to_binary(RESP.encode(["XLEN", "pipe"]))
    :ok = :gen_tcp.send(socket, cmd1 <> cmd2)

    [id, len] = recv_n(socket, <<>>, 2)

    assert is_binary(id)
    assert len == 1
  end

  # ---------------------------------------------------------------------------
  # Blocking XREADGROUP
  # ---------------------------------------------------------------------------

  test "blocking XREADGROUP resolves when task is pushed from another connection",
       %{socket: socket, port: port, data_dir: data_dir} do
    # Second connection for pushing
    pusher = tcp_connect(port)

    on_exit(fn -> :gen_tcp.close(pusher) end)

    # Start async blocking read (BLOCK 3000ms)
    parent = self()

    reader =
      Task.async(fn ->
        result =
          send_command!(socket, ["XREADGROUP", "GROUP", "workers", "w1",
                                  "COUNT", "1", "BLOCK", "3000", "STREAMS", "bq", ">"])
        send(parent, {:reader_result, result})
      end)

    # Give the blocking read time to register in Engine
    Process.sleep(100)

    # Push from second connection
    pushed_id = send_command!(pusher, ["XADD", "bq", "*", "payload", "wake"])

    # Blocking read should resolve with the task
    assert_receive {:reader_result, result}, 4_000
    assert [["bq", [[^pushed_id, fields]]]] = result
    assert field_value(fields, "payload") == "wake"

    Task.await(reader)
    _ = data_dir
  end

  test "blocking XREADGROUP returns null on timeout", %{socket: socket} do
    result = send_command!(socket, ["XREADGROUP", "GROUP", "workers", "w1",
                                     "COUNT", "1", "BLOCK", "100", "STREAMS", "empty_q", ">"],
                           timeout: 2_000)
    assert is_nil(result)
  end

  test "engine stays healthy after connection drops during blocking pop",
       %{socket: healthy, port: port} do
    dropper = tcp_connect(port)

    on_exit(fn -> :gen_tcp.close(dropper) end)

    # Start a short blocking pop from the connection we're about to drop.
    # We fire-and-forget since the dropper socket will close before it returns.
    Task.start(fn ->
      wire = IO.iodata_to_binary(RESP.encode(
        ["XREADGROUP", "GROUP", "workers", "dropper",
         "COUNT", "1", "BLOCK", "150", "STREAMS", "drop_q", ">"]))
      :gen_tcp.send(dropper, wire)
    end)

    # Give it time to register in the Engine waiters map
    Process.sleep(100)

    # Abruptly close the connection - simulates a client crash
    :gen_tcp.close(dropper)

    # Wait for the block to expire and the connection process to finish cleanup
    Process.sleep(300)

    # Engine must still be healthy: push and pop should work normally
    id = send_command!(healthy, ["XADD", "drop_q", "*", "payload", "recovery"])
    assert is_binary(id)

    result = send_command!(healthy, ["XREADGROUP", "GROUP", "workers", "w2",
                                     "COUNT", "1", "STREAMS", "drop_q", ">"])
    assert [["drop_q", [[^id, _]]]] = result
  end

  # ---------------------------------------------------------------------------
  # Error handling - connection must survive bad input
  # ---------------------------------------------------------------------------

  test "malformed RESP bytes return a protocol error and connection survives", %{socket: socket} do
    # "!" is not a valid RESP type prefix; decode returns {:error, ...}
    :ok = :gen_tcp.send(socket, "!bad\r\n")
    resp = recv_one(socket, <<>>, 1_000)
    assert {:error, _} = resp

    id = send_command!(socket, ["XADD", "q", "*", "payload", "after_error"])
    assert is_binary(id)
  end

  test "command with wrong argument count returns error and connection survives", %{socket: socket} do
    # XADD with no queue name - falls through to {:unknown, tokens}
    resp = send_command!(socket, ["XADD"])
    assert {:error, _} = resp

    id = send_command!(socket, ["XADD", "q", "*", "payload", "ok"])
    assert is_binary(id)
  end

  # ---------------------------------------------------------------------------
  # Private helpers
  # ---------------------------------------------------------------------------

  # Send a RESP array command and decode the single response.
  defp send_command!(socket, tokens, opts \\ []) do
    wire = IO.iodata_to_binary(RESP.encode(tokens))
    :ok = :gen_tcp.send(socket, wire)
    recv_one(socket, <<>>, Keyword.get(opts, :timeout, 1_000))
  end

  defp recv_one(socket, buf, timeout) do
    case RESP.decode(buf) do
      {:ok, value, _rest} ->
        value

      {:more, _} ->
        case :gen_tcp.recv(socket, 0, timeout) do
          {:ok, data} -> recv_one(socket, buf <> data, timeout)
          {:error, reason} -> raise "TCP recv failed: #{inspect(reason)}"
        end
    end
  end

  # Decode `count` consecutive RESP frames from the socket, threading the
  # leftover buffer through each decode so pipelined responses that arrive in
  # a single TCP segment are handled correctly.
  defp recv_n(socket, buf, count, acc \\ [])
  defp recv_n(_socket, _buf, 0, acc), do: Enum.reverse(acc)
  defp recv_n(socket, buf, n, acc) do
    case RESP.decode(buf) do
      {:ok, value, rest} ->
        recv_n(socket, rest, n - 1, [value | acc])

      {:more, _} ->
        case :gen_tcp.recv(socket, 0, 1_000) do
          {:ok, data} -> recv_n(socket, buf <> data, n, acc)
          {:error, reason} -> raise "TCP recv failed: #{inspect(reason)}"
        end
    end
  end

  # Extract a value from a flat [key, val, key, val, ...] list.
  defp field_value(list, key) do
    idx = Enum.find_index(list, &(&1 == key))
    if idx, do: Enum.at(list, idx + 1), else: nil
  end

  defp free_port do
    {:ok, sock} = :gen_tcp.listen(0, [])
    {:ok, port} = :inet.port(sock)
    :gen_tcp.close(sock)
    port
  end

  defp tcp_connect(port, retries \\ 20) do
    case :gen_tcp.connect(~c"127.0.0.1", port, [:binary, {:active, false}, {:packet, :raw}], 200) do
      {:ok, socket} ->
        socket

      {:error, _} when retries > 0 ->
        Process.sleep(25)
        tcp_connect(port, retries - 1)

      {:error, reason} ->
        raise "TCP connect failed after retries: #{inspect(reason)}"
    end
  end
end
