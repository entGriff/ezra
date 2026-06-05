defmodule Ezra.Server.RESP do
  @moduledoc false

  # RESP3 wire protocol - encode and decode.
  #
  # Only the subset needed for EZRA's Redis Streams command surface:
  #   XADD, XGROUP, XREADGROUP, XACK, XLEN, XINFO STREAM
  #
  # decode/1 returns {:ok, value, rest} | {:more, binary()} | {:error, reason}
  # encode/1 returns iodata()

  @crlf "\r\n"
  @version Mix.Project.config()[:version]

  # ---------------------------------------------------------------------------
  # Encode
  # ---------------------------------------------------------------------------

  @spec encode(term()) :: iodata()

  # RESP3 null
  def encode(:null), do: "_\r\n"
  def encode(:ok), do: "+OK\r\n"

  def encode({:simple, str}) when is_binary(str), do: ["+", str, @crlf]
  def encode({:error, msg}) when is_binary(msg), do: ["-", msg, @crlf]

  def encode(n) when is_integer(n), do: [":", Integer.to_string(n), @crlf]

  # Null bulk string (RESP2 compat - what redis-py expects for nil values)
  def encode(nil), do: "$-1\r\n"

  def encode(bin) when is_binary(bin) do
    ["$", Integer.to_string(byte_size(bin)), @crlf, bin, @crlf]
  end

  def encode(list) when is_list(list) do
    ["*", Integer.to_string(length(list)), @crlf | Enum.map(list, &encode/1)]
  end

  def encode(%{} = map) do
    entries = Enum.flat_map(map, fn {k, v} -> [encode(k), encode(v)] end)
    ["%", Integer.to_string(map_size(map)), @crlf | entries]
  end

  # ---------------------------------------------------------------------------
  # Decode
  # ---------------------------------------------------------------------------

  @type decode_result ::
          {:ok, term(), binary()}
          | {:more, binary()}
          | {:error, binary()}

  @spec decode(binary()) :: decode_result()

  def decode(data) when is_binary(data) do
    case data do
      "+" <> rest -> decode_line(rest, &{:ok, {:simple, &1}, &2})
      "-" <> rest -> decode_line(rest, &{:ok, {:error, &1}, &2})
      ":" <> rest -> decode_line(rest, &parse_int(&1, &2))
      "$" <> rest -> decode_bulk(rest)
      "*" <> rest -> decode_array(rest)
      "%" <> rest -> decode_map(rest)
      "_\r\n" <> rest -> {:ok, nil, rest}
      _ when byte_size(data) < 3 -> {:more, data}
      _ -> {:error, "unknown type prefix"}
    end
  end

  # ---------------------------------------------------------------------------
  # Parse commands
  # ---------------------------------------------------------------------------

  # Accepts a decoded RESP array of binaries and returns a structured command
  # tuple, or {:unknown, tokens} for anything not in EZRA's command surface.

  @spec parse_command([binary()]) :: term()

  def parse_command(tokens) when is_list(tokens) do
    # Match on uppercased tokens for case-insensitive command names,
    # but always extract values from the original `tokens` to preserve case.
    upped = Enum.map(tokens, &String.upcase/1)

    case upped do
      # HELLO [protover [AUTH username password] [SETNAME clientname]]
      # AUTH and SETNAME sub-options are accepted and ignored.
      ["HELLO"] ->
        {:hello, 2}

      ["HELLO", v | _] ->
        {:hello, parse_hello_proto(v)}

      # PING [message]
      ["PING"] ->
        {:ping, nil}

      ["PING", _msg] ->
        {:ping, Enum.at(tokens, 1)}

      # CLIENT SETNAME <name>  (no-op - accepted for SDK compatibility)
      ["CLIENT", "SETNAME", _] ->
        {:client_setname}

      # CLIENT NO-EVICT / NO-TOUCH - node-redis sends these on every connect
      ["CLIENT", "NO-EVICT" | _] ->
        {:client_no_evict}

      ["CLIENT", "NO-TOUCH" | _] ->
        {:client_no_touch}

      # CLIENT GETNAME / ID - debugging / introspection sub-commands
      ["CLIENT", "GETNAME"] ->
        {:client_getname}

      ["CLIENT", "ID"] ->
        {:client_id}

      # COMMAND [DOCS|INFO|COUNT|...] - clients probe for command availability
      ["COMMAND" | _] ->
        {:command}

      # RESET - resets connection state (Redis 6.2+); some connection pools send it
      ["RESET"] ->
        {:reset}

      # SELECT n - accepted as no-op (EZRA has no databases)
      ["SELECT" | _] ->
        {:select}

      # INFO [section] - ioredis sends this on connect as a ready-check
      ["INFO" | _] ->
        {:info}

      # XADD <queue> * payload <data> [field value ...]
      ["XADD", _queue, _id | _] ->
        {:xadd, Enum.at(tokens, 1), parse_fields(Enum.drop(tokens, 3))}

      # XGROUP CREATE <queue> <group> $ [MKSTREAM]
      ["XGROUP", "CREATE" | _] ->
        {:xgroup_create, Enum.at(tokens, 2), Enum.at(tokens, 3)}

      # XREADGROUP GROUP <group> <consumer> COUNT n [BLOCK ms] STREAMS <queue> >
      ["XREADGROUP", "GROUP" | _] ->
        group = Enum.at(tokens, 2)
        consumer = Enum.at(tokens, 3)
        rest_norm = Enum.drop(upped, 4)
        rest_orig = Enum.drop(tokens, 4)
        {count, block_ms, queue} = extract_xreadgroup_opts(rest_norm, rest_orig)
        {:xreadgroup, group, consumer, queue, count, block_ms}

      # XACK <queue> <group> <id>
      ["XACK", _, _, _] ->
        {:xack, Enum.at(tokens, 1), Enum.at(tokens, 3)}

      # XNACK <queue> <group> <id>
      ["XNACK", _, _, _] ->
        {:xnack, Enum.at(tokens, 1), Enum.at(tokens, 3)}

      # XDEL <queue> <id>  - treated as nack in EZRA (returns task for retry)
      ["XDEL", _, _ | _] ->
        {:xdel_nack, Enum.at(tokens, 1), Enum.at(tokens, 2)}

      # XLEN <queue>
      ["XLEN", _] ->
        {:xlen, Enum.at(tokens, 1)}

      # XINFO STREAM <queue>
      ["XINFO", "STREAM", _] ->
        {:xinfo_stream, Enum.at(tokens, 2)}

      _ ->
        {:unknown, tokens}
    end
  end

  # ---------------------------------------------------------------------------
  # Response encoders
  # ---------------------------------------------------------------------------

  # XADD → task id as bulk string
  def encode_push_response(task_id) when is_binary(task_id), do: encode(task_id)

  # XREADGROUP response format depends on negotiated protocol.
  #
  # RESP2 (proto < 3): flat nested array [[stream, [[id, fields]]]]
  #   redis-py calls parse_stream / parse_xread on this.
  #
  # RESP3 (proto >= 3): map %{stream => [[id, fields]]}
  #   redis-py calls parse_xread_resp3 which iterates .items() and wraps
  #   the message list in an extra []. Caller iterates with (messages,) unpacking.
  #
  # 2-arg form defaults to RESP2 (used by tests and non-HELLO connections).
  def encode_pop_response(queue, task), do: encode_pop_response(queue, task, 2)

  # RESP3 null for empty/timeout: redis-py's RESP3 parser does not treat $-1 as
  # null (that's RESP2-only). It must receive the native RESP3 null type (_\r\n).
  def encode_pop_response(_queue, nil, proto) when proto >= 3 do
    encode(:null)
  end

  def encode_pop_response(queue, task, proto) when proto >= 3 do
    fields = [
      "payload", task.payload,
      "attempts", Integer.to_string(task.attempts),
      "max_attempts", Integer.to_string(task.max_attempts)
    ]
    encode(%{queue => [[task.id, fields]]})
  end

  # RESP2: null means no messages - return null so workers see an empty result
  def encode_pop_response(_queue, nil, _proto) do
    encode(nil)
  end

  def encode_pop_response(queue, task, _proto) do
    fields = [
      "payload", task.payload,
      "attempts", Integer.to_string(task.attempts),
      "max_attempts", Integer.to_string(task.max_attempts)
    ]
    encode([[queue, [[task.id, fields]]]])
  end

  # Blocking XREADGROUP timeout → null. RESP3 parser requires native null (_\r\n);
  # it does not handle $-1 as null the way the RESP2 parser does.
  def encode_block_timeout(proto) when proto >= 3, do: encode(:null)
  def encode_block_timeout(_proto), do: encode(nil)

  # XACK → integer 1 (acked) or 0 (not found)
  def encode_xack_response(:ok), do: encode(1)
  def encode_xack_response(:not_found), do: encode(0)

  # XLEN → integer depth
  def encode_xlen_response(n), do: encode(n)

  # XINFO STREAM → flat array (RESP2) or map (RESP3).
  # Includes the standard Redis fields so clients like RedisInsight parse cleanly.
  def encode_xinfo_response(info), do: encode_xinfo_response(info, 2)

  def encode_xinfo_response(%{queue: queue, length: length, dead: dead} = info, proto) do
    last_id = Map.get(info, :last_id)
    last_id_str = if last_id, do: Integer.to_string(last_id), else: "0-0"
    null_val = if proto >= 3, do: :null, else: nil

    pairs = [
      {"name",                    queue},
      {"length",                  length},
      {"radix-tree-keys",         0},
      {"radix-tree-nodes",        1},
      {"last-generated-id",       last_id_str},
      {"max-deleted-entry-id",    "0-0"},
      {"entries-added",           length},
      {"recorded-first-entry-id", "0-0"},
      {"groups",                  1},
      {"dead-letter-length",      dead},
      {"first-entry",             null_val},
      {"last-entry",              null_val}
    ]

    if proto >= 3 do
      encode(Map.new(pairs))
    else
      encode(Enum.flat_map(pairs, fn {k, v} -> [k, v] end))
    end
  end

  # HELLO response format depends on the requested version:
  #
  #   HELLO 3+ → RESP3 map (%N). redis-py 5.x in protocol=3 mode calls
  #              handshake_metadata.get(b"proto") and requires a dict, not a list.
  #              We confirm proto:3 so the client stays happy; our actual command
  #              responses use RESP2 wire types (bulk strings, integers, arrays),
  #              which are all valid RESP3 and parsed correctly by any RESP3 client.
  #
  #   HELLO 2 / HELLO → RESP2 flat array (*N). Classic clients interpret this as
  #              a key-value list and extract proto:2.
  #
  def encode_hello(v) when is_integer(v) and v >= 3 do
    encode(%{
      "server"  => "ezra",
      "version" => @version,
      "proto"   => 3,
      "id"      => 0,
      "mode"    => "standalone",
      "role"    => "master",
      "modules" => []
    })
  end

  def encode_hello(_v) do
    encode([
      "server",  "ezra",
      "version", @version,
      "proto",   2,
      "id",      0,
      "mode",    "standalone",
      "role",    "master",
      "modules", []
    ])
  end

  # INFO stub - enough for ioredis and other clients that send INFO as a ready-check.
  # Returns a minimal bulk-string response that satisfies version/loading checks.
  def encode_info() do
    body = "# Server\r\nredis_version:7.0.0\r\nredis_mode:standalone\r\nloading:0\r\n# Replication\r\nrole:master\r\n"
    encode(body)
  end

  def encode_error(msg) when is_binary(msg), do: encode({:error, msg})
  def encode_ok(), do: encode(:ok)

  # ---------------------------------------------------------------------------
  # Private - decode helpers
  # ---------------------------------------------------------------------------

  defp decode_line(data, cont) do
    case :binary.split(data, @crlf) do
      [line, rest] -> cont.(line, rest)
      _ -> {:more, data}
    end
  end

  defp parse_int(str, rest) do
    case Integer.parse(str) do
      {n, ""} -> {:ok, n, rest}
      _ -> {:error, "invalid integer: #{str}"}
    end
  end

  defp decode_bulk(data) do
    decode_line(data, fn len_str, rest ->
      case Integer.parse(len_str) do
        {-1, ""} ->
          {:ok, nil, rest}

        {len, ""} when len >= 0 ->
          case rest do
            <<str::binary-size(len), "\r\n", tail::binary>> ->
              {:ok, str, tail}

            _ when byte_size(rest) < len + 2 ->
              {:more, "$" <> len_str <> @crlf <> rest}

            _ ->
              {:error, "bulk string missing CRLF terminator"}
          end

        _ ->
          {:error, "invalid bulk length: #{len_str}"}
      end
    end)
  end

  defp decode_array(data) do
    decode_line(data, fn len_str, rest ->
      case Integer.parse(len_str) do
        {-1, ""} -> {:ok, nil, rest}
        {0, ""} -> {:ok, [], rest}
        {len, ""} when len > 0 -> decode_n(len, rest, [])
        _ -> {:error, "invalid array length: #{len_str}"}
      end
    end)
  end

  defp decode_map(data) do
    decode_line(data, fn len_str, rest ->
      case Integer.parse(len_str) do
        {0, ""} -> {:ok, %{}, rest}
        {len, ""} when len > 0 -> decode_map_entries(len, rest, %{})
        _ -> {:error, "invalid map length: #{len_str}"}
      end
    end)
  end

  defp decode_n(0, rest, acc), do: {:ok, Enum.reverse(acc), rest}

  defp decode_n(n, data, acc) do
    case decode(data) do
      {:ok, value, rest} -> decode_n(n - 1, rest, [value | acc])
      {:more, _} -> {:more, data}
      {:error, _} = err -> err
    end
  end

  defp decode_map_entries(0, rest, acc), do: {:ok, acc, rest}

  defp decode_map_entries(n, data, acc) do
    with {:ok, key, rest1} <- decode(data),
         {:ok, val, rest2} <- decode(rest1) do
      decode_map_entries(n - 1, rest2, Map.put(acc, key, val))
    else
      {:more, _} -> {:more, data}
      {:error, _} = err -> err
    end
  end

  # ---------------------------------------------------------------------------
  # Private - command parse helpers
  # ---------------------------------------------------------------------------

  defp parse_hello_proto(v) do
    case Integer.parse(v) do
      {n, ""} -> n
      _ -> 2
    end
  end

  defp parse_fields(fields) do
    fields
    |> Enum.chunk_every(2)
    |> Enum.into(%{}, fn
      [k, v] -> {String.downcase(k), v}
      [k] -> {String.downcase(k), nil}
    end)
  end

  defp extract_xreadgroup_opts(norm, orig) do
    {count, norm, orig} = extract_opt(norm, orig, "COUNT", 1)
    {block_ms, norm, orig} = extract_opt(norm, orig, "BLOCK", 0)

    queue =
      case Enum.find_index(norm, &(&1 == "STREAMS")) do
        nil -> nil
        i -> Enum.at(orig, i + 1)
      end

    {count, block_ms, queue}
  end

  defp extract_opt(norm, orig, key, default) do
    case Enum.find_index(norm, &(&1 == key)) do
      nil ->
        {default, norm, orig}

      i ->
        val = orig |> Enum.at(i + 1) |> then(&if(&1, do: String.to_integer(&1), else: default))
        norm2 = norm |> List.delete_at(i + 1) |> List.delete_at(i)
        orig2 = orig |> List.delete_at(i + 1) |> List.delete_at(i)
        {val, norm2, orig2}
    end
  end
end
