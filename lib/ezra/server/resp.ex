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

  # XREADGROUP → [[stream_name, [[id, [field, val, ...]]]]]
  # Matches Redis wire format so redis-py parses without any wrapper code.
  def encode_pop_response(queue, nil) do
    # Non-blocking read with no messages: return stream with empty entries list.
    encode([[queue, []]])
  end

  def encode_pop_response(queue, task) do
    fields = [
      "payload", task.payload,
      "attempts", Integer.to_string(task.attempts),
      "max_attempts", Integer.to_string(task.max_attempts)
    ]

    encode([[queue, [[task.id, fields]]]])
  end

  # Blocking XREADGROUP timeout → null (redis-py checks for None)
  def encode_block_timeout(), do: encode(nil)

  # XACK → integer 1 (acked) or 0 (not found)
  def encode_xack_response(:ok), do: encode(1)
  def encode_xack_response(:not_found), do: encode(0)

  # XLEN → integer depth
  def encode_xlen_response(n), do: encode(n)

  # XINFO STREAM → flat array matching Redis wire format for GUI compatibility.
  # Includes the standard Redis fields so clients like RedisInsight parse cleanly.
  def encode_xinfo_response(%{queue: queue, length: length, dead: dead} = info) do
    last_id = Map.get(info, :last_id)
    last_id_str = if last_id, do: Integer.to_string(last_id), else: "0-0"
    total = length

    encode([
      "name",                    queue,
      "length",                  total,
      "radix-tree-keys",         0,
      "radix-tree-nodes",        1,
      "last-generated-id",       last_id_str,
      "max-deleted-entry-id",    "0-0",
      "entries-added",           total,
      "recorded-first-entry-id", "0-0",
      "groups",                  1,
      "dead-letter-length",      dead,
      "first-entry",             nil,
      "last-entry",              nil
    ])
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
