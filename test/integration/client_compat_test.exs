defmodule Ezra.Integration.ClientCompatTest do
  @moduledoc false

  # Tests that simulate the exact connection sequences of real Redis client
  # libraries. Written from the client's point of view, not EZRA's.
  #
  # The goal: any change that breaks a real client fails here, before Docker.
  #
  # Clients covered:
  #   redis-py 5.x  — HELLO 3 on every connect (RESP3 request)
  #   go-redis       — HELLO 3 + CLIENT SETNAME
  #   ioredis        — CLIENT SETNAME, no HELLO
  #   minimal        — no handshake at all (raw RESP2)

  use ExUnit.Case, async: true

  alias Ezra.Server.RESP

  setup do
    port = free_port()
    uid = System.unique_integer([:positive])
    name = :"ezra_compat_#{uid}"
    data_dir = "/tmp/ezra_compat_#{uid}"

    start_supervised!({Ezra, name: name, data_dir: data_dir, port: port})

    on_exit(fn -> File.rm_rf!(data_dir) end)

    %{port: port}
  end

  # --- redis-py 5.x ---
  # Sends HELLO 3 on every new connection. If the server rejects it the
  # connection fails before user code runs - which is how we first found
  # the NOPROTO bug.

  test "redis-py 5.x: HELLO 3 → XADD → XREADGROUP → XACK", %{port: port} do
    sock = connect(port)

    hello = cmd!(sock, ["HELLO", "3"])
    assert is_map(hello)
    assert Map.get(hello, "proto") == 3
    assert Map.get(hello, "server") == "ezra"

    id = cmd!(sock, ["XADD", "jobs", "*", "payload", "do-work"])
    assert String.match?(id, ~r/^\d+$/)

    # RESP3: XREADGROUP returns a map %{stream => [[id, fields]]}
    %{"jobs" => [[^id, fields]]} = cmd!(sock, ["XREADGROUP", "GROUP", "g", "w1",
                                               "COUNT", "1", "STREAMS", "jobs", ">"])
    assert field(fields, "payload") == "do-work"

    assert 1 = cmd!(sock, ["XACK", "jobs", "g", id])
  end

  test "redis-py 5.x: HELLO 3 + CLIENT SETNAME preamble", %{port: port} do
    sock = connect(port)

    # redis-py sets the connection name when pool_name is configured
    cmd!(sock, ["HELLO", "3"])
    assert {:simple, "OK"} = cmd!(sock, ["CLIENT", "SETNAME", "myapp-worker-1"])

    id = cmd!(sock, ["XADD", "q", "*", "payload", "named-worker"])
    %{"q" => [[^id, _]]} = cmd!(sock, ["XREADGROUP", "GROUP", "g", "myapp-worker-1",
                                       "COUNT", "1", "STREAMS", "q", ">"])
  end

  test "redis-py 5.x: HELLO 3 then XNACK returns task for retry", %{port: port} do
    sock = connect(port)
    cmd!(sock, ["HELLO", "3"])

    id = cmd!(sock, ["XADD", "q", "*", "payload", "try-me"])
    cmd!(sock, ["XREADGROUP", "GROUP", "g", "w", "COUNT", "1", "STREAMS", "q", ">"])

    # Worker signals failure
    assert {:simple, "OK"} = cmd!(sock, ["XNACK", "q", "g", id])

    # Task must be retrievable again
    %{"q" => [[^id, _]]} = cmd!(sock, ["XREADGROUP", "GROUP", "g", "w",
                                       "COUNT", "1", "STREAMS", "q", ">"])
  end

  test "redis-py 5.x: blocking pop resolves when another connection pushes",
       %{port: port} do
    consumer = connect(port)
    pusher   = connect(port)

    cmd!(consumer, ["HELLO", "3"])
    cmd!(pusher,   ["HELLO", "3"])

    parent = self()

    Task.async(fn ->
      result = cmd!(consumer, ["XREADGROUP", "GROUP", "g", "w",
                               "COUNT", "1", "BLOCK", "3000", "STREAMS", "bq", ">"],
                   timeout: 5_000)
      send(parent, {:pop, result})
    end)

    Process.sleep(100)
    pushed_id = cmd!(pusher, ["XADD", "bq", "*", "payload", "wake"])

    assert_receive {:pop, %{"bq" => [[^pushed_id, _]]}}, 4_000
  end

  # --- go-redis ---
  # Sends HELLO 3 followed by CLIENT SETNAME on every connection.

  test "go-redis: HELLO 3 + CLIENT SETNAME + full workflow", %{port: port} do
    sock = connect(port)

    hello = cmd!(sock, ["HELLO", "3"])
    assert is_map(hello)
    assert Map.get(hello, "proto") == 3

    cmd!(sock, ["CLIENT", "SETNAME", "go-redis-worker"])

    id = cmd!(sock, ["XADD", "tasks", "*", "payload", "from-go"])
    %{"tasks" => [[^id, _]]} = cmd!(sock, ["XREADGROUP", "GROUP", "g", "go-redis-worker",
                                           "COUNT", "1", "STREAMS", "tasks", ">"])
    assert 1 = cmd!(sock, ["XACK", "tasks", "g", id])
  end

  # --- ioredis (Node.js) ---
  # Typically sends CLIENT SETNAME without HELLO. No protocol negotiation.

  test "ioredis: CLIENT SETNAME only, then full workflow", %{port: port} do
    sock = connect(port)

    assert {:simple, "OK"} = cmd!(sock, ["CLIENT", "SETNAME", "ioredis-consumer"])

    id = cmd!(sock, ["XADD", "q", "*", "payload", "from-node"])
    [[_, [[^id, fields]]]] = cmd!(sock, ["XREADGROUP", "GROUP", "g", "ioredis-consumer",
                                         "COUNT", "1", "STREAMS", "q", ">"])
    assert field(fields, "payload") == "from-node"
    assert 1 = cmd!(sock, ["XACK", "q", "g", id])
  end

  # --- Minimal / raw RESP2 client ---
  # No handshake at all - just commands. Old clients, simple scripts, curl-style.

  test "minimal client: no handshake, XADD → XREADGROUP → XACK", %{port: port} do
    sock = connect(port)

    id = cmd!(sock, ["XADD", "q", "*", "payload", "raw"])
    [[_, [[^id, _]]]] = cmd!(sock, ["XREADGROUP", "GROUP", "g", "w",
                                    "COUNT", "1", "STREAMS", "q", ">"])
    assert 1 = cmd!(sock, ["XACK", "q", "g", id])
  end

  # --- Health-checking clients ---
  # Many SDKs and proxies send PING between commands to verify the connection.

  test "PING interleaved with task workflow", %{port: port} do
    sock = connect(port)
    cmd!(sock, ["HELLO", "3"])

    ids =
      for i <- 1..3 do
        assert {:simple, "PONG"} = cmd!(sock, ["PING"])
        cmd!(sock, ["XADD", "jobs", "*", "payload", "task-#{i}"])
      end

    for id <- ids do
      assert {:simple, "PONG"} = cmd!(sock, ["PING"])
      cmd!(sock, ["XREADGROUP", "GROUP", "g", "w", "COUNT", "1", "STREAMS", "jobs", ">"])
      cmd!(sock, ["XACK", "jobs", "g", id])
    end
  end

  test "PING with message echoes back", %{port: port} do
    sock = connect(port)
    cmd!(sock, ["HELLO", "3"])
    assert "health-check" = cmd!(sock, ["PING", "health-check"])
  end

  # --- XINFO / observability ---

  test "XINFO STREAM after workflow returns correct counts", %{port: port} do
    sock = connect(port)
    cmd!(sock, ["HELLO", "3"])

    cmd!(sock, ["XADD", "q", "*", "payload", "a"])
    cmd!(sock, ["XADD", "q", "*", "payload", "b"])

    info = cmd!(sock, ["XINFO", "STREAM", "q"])
    assert field(info, "length") == 2
    assert field(info, "name") == "q"
  end

  # --- Helpers ---

  defp cmd!(socket, tokens, opts \\ []) do
    :ok = :gen_tcp.send(socket, IO.iodata_to_binary(RESP.encode(tokens)))
    recv_one(socket, <<>>, Keyword.get(opts, :timeout, 1_000))
  end

  defp recv_one(socket, buf, timeout) do
    case RESP.decode(buf) do
      {:ok, value, _} ->
        value

      {:more, _} ->
        case :gen_tcp.recv(socket, 0, timeout) do
          {:ok, data} -> recv_one(socket, buf <> data, timeout)
          {:error, reason} -> raise "recv failed: #{inspect(reason)}"
        end
    end
  end

  # Extract a value from a flat [key, val, key, val, ...] list.
  defp field(list, key) do
    idx = Enum.find_index(list, &(&1 == key))
    if idx, do: Enum.at(list, idx + 1), else: nil
  end

  defp free_port do
    {:ok, sock} = :gen_tcp.listen(0, [])
    {:ok, port} = :inet.port(sock)
    :gen_tcp.close(sock)
    port
  end

  defp connect(port, retries \\ 20) do
    case :gen_tcp.connect(~c"127.0.0.1", port, [:binary, {:active, false}, {:packet, :raw}], 200) do
      {:ok, socket} ->
        socket

      {:error, _} when retries > 0 ->
        Process.sleep(25)
        connect(port, retries - 1)

      {:error, reason} ->
        raise "connect failed: #{inspect(reason)}"
    end
  end
end
