defmodule Ezra.Server.RESPTest do
  use ExUnit.Case, async: true

  alias Ezra.Server.RESP

  # ---------------------------------------------------------------------------
  # Encode - all types
  # ---------------------------------------------------------------------------

  describe "encode/1" do
    test "simple string" do
      assert IO.iodata_to_binary(RESP.encode({:simple, "OK"})) == "+OK\r\n"
    end

    test "error" do
      assert IO.iodata_to_binary(RESP.encode({:error, "ERR bad"})) == "-ERR bad\r\n"
    end

    test "integer" do
      assert IO.iodata_to_binary(RESP.encode(42)) == ":42\r\n"
      assert IO.iodata_to_binary(RESP.encode(0)) == ":0\r\n"
      assert IO.iodata_to_binary(RESP.encode(-1)) == ":-1\r\n"
    end

    test "bulk string" do
      assert IO.iodata_to_binary(RESP.encode("hello")) == "$5\r\nhello\r\n"
      assert IO.iodata_to_binary(RESP.encode("")) == "$0\r\n\r\n"
    end

    test "null bulk string" do
      assert IO.iodata_to_binary(RESP.encode(nil)) == "$-1\r\n"
    end

    test "RESP3 null atom" do
      assert IO.iodata_to_binary(RESP.encode(:null)) == "_\r\n"
    end

    test "ok atom" do
      assert IO.iodata_to_binary(RESP.encode(:ok)) == "+OK\r\n"
    end

    test "empty array" do
      assert IO.iodata_to_binary(RESP.encode([])) == "*0\r\n"
    end

    test "array of bulk strings" do
      result = IO.iodata_to_binary(RESP.encode(["foo", "bar"]))
      assert result == "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"
    end

    test "nested array" do
      result = IO.iodata_to_binary(RESP.encode([["a", "b"], ["c"]]))
      assert result == "*2\r\n*2\r\n$1\r\na\r\n$1\r\nb\r\n*1\r\n$1\r\nc\r\n"
    end

    test "map" do
      result = IO.iodata_to_binary(RESP.encode(%{"k" => "v"}))
      assert result == "%1\r\n$1\r\nk\r\n$1\r\nv\r\n"
    end
  end

  # ---------------------------------------------------------------------------
  # Decode - all types
  # ---------------------------------------------------------------------------

  describe "decode/1" do
    test "simple string" do
      assert RESP.decode("+OK\r\n") == {:ok, {:simple, "OK"}, ""}
    end

    test "error" do
      assert RESP.decode("-ERR bad\r\n") == {:ok, {:error, "ERR bad"}, ""}
    end

    test "integer" do
      assert RESP.decode(":42\r\n") == {:ok, 42, ""}
      assert RESP.decode(":-1\r\n") == {:ok, -1, ""}
    end

    test "bulk string" do
      assert RESP.decode("$5\r\nhello\r\n") == {:ok, "hello", ""}
      assert RESP.decode("$0\r\n\r\n") == {:ok, "", ""}
    end

    test "null bulk string" do
      assert RESP.decode("$-1\r\n") == {:ok, nil, ""}
    end

    test "RESP3 null" do
      assert RESP.decode("_\r\n") == {:ok, nil, ""}
    end

    test "empty array" do
      assert RESP.decode("*0\r\n") == {:ok, [], ""}
    end

    test "null array" do
      assert RESP.decode("*-1\r\n") == {:ok, nil, ""}
    end

    test "array of bulk strings" do
      assert RESP.decode("*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n") ==
               {:ok, ["foo", "bar"], ""}
    end

    test "map" do
      assert RESP.decode("%1\r\n$1\r\nk\r\n$1\r\nv\r\n") ==
               {:ok, %{"k" => "v"}, ""}
    end

    test "leftover bytes returned as rest" do
      assert RESP.decode(":1\r\n:2\r\n") == {:ok, 1, ":2\r\n"}
    end

    test "unknown prefix returns error" do
      assert RESP.decode("!bad\r\n") == {:error, "unknown type prefix"}
    end
  end

  # ---------------------------------------------------------------------------
  # Partial reads - {:more, _} accumulation
  # ---------------------------------------------------------------------------

  describe "partial reads" do
    test "incomplete simple string" do
      assert {:more, _} = RESP.decode("+OK")
    end

    test "incomplete bulk string header" do
      assert {:more, _} = RESP.decode("$5")
    end

    test "bulk string body not yet fully arrived" do
      # header complete, body truncated
      assert {:more, _} = RESP.decode("$5\r\nhel")
    end

    test "incomplete array" do
      # header says 2 elements, only 1 arrived
      assert {:more, _} = RESP.decode("*2\r\n$3\r\nfoo\r\n")
    end

    test "empty binary" do
      assert {:more, ""} = RESP.decode("")
    end

    test "single byte" do
      assert {:more, _} = RESP.decode("$")
    end
  end

  # ---------------------------------------------------------------------------
  # Roundtrip - encode then decode
  # ---------------------------------------------------------------------------

  describe "roundtrip" do
    test "bulk string" do
      wire = IO.iodata_to_binary(RESP.encode("hello world"))
      assert RESP.decode(wire) == {:ok, "hello world", ""}
    end

    test "integer" do
      wire = IO.iodata_to_binary(RESP.encode(12_345))
      assert RESP.decode(wire) == {:ok, 12_345, ""}
    end

    test "array of mixed types" do
      wire = IO.iodata_to_binary(RESP.encode(["XADD", "emails", "*", "payload", "hi"]))
      assert RESP.decode(wire) == {:ok, ["XADD", "emails", "*", "payload", "hi"], ""}
    end

    test "nested array" do
      val = [["id-1", ["payload", "data", "attempts", "1"]]]
      wire = IO.iodata_to_binary(RESP.encode(val))
      assert {:ok, decoded, ""} = RESP.decode(wire)
      assert decoded == val
    end
  end

  # ---------------------------------------------------------------------------
  # parse_command/1
  # ---------------------------------------------------------------------------

  describe "parse_command/1" do
    test "XADD extracts queue and payload field" do
      assert RESP.parse_command(["XADD", "emails", "*", "payload", "hello"]) ==
               {:xadd, "emails", %{"payload" => "hello"}}
    end

    test "XADD is case-insensitive for command name" do
      assert {:xadd, "q", _} = RESP.parse_command(["xadd", "q", "*", "payload", "x"])
    end

    test "XADD with multiple fields" do
      assert {:xadd, "q", fields} =
               RESP.parse_command(["XADD", "q", "*", "payload", "data", "ttl", "60"])

      assert fields == %{"payload" => "data", "ttl" => "60"}
    end

    test "XGROUP CREATE" do
      assert RESP.parse_command(["XGROUP", "CREATE", "emails", "workers", "$", "MKSTREAM"]) ==
               {:xgroup_create, "emails", "workers"}
    end

    test "XREADGROUP without BLOCK" do
      tokens = ["XREADGROUP", "GROUP", "workers", "w1", "COUNT", "1", "STREAMS", "emails", ">"]
      assert {:xreadgroup, "workers", "w1", "emails", 1, 0} = RESP.parse_command(tokens)
    end

    test "XREADGROUP with BLOCK" do
      tokens = [
        "XREADGROUP", "GROUP", "workers", "w1",
        "COUNT", "1", "BLOCK", "5000", "STREAMS", "emails", ">"
      ]

      assert {:xreadgroup, "workers", "w1", "emails", 1, 5000} = RESP.parse_command(tokens)
    end

    test "XACK" do
      assert RESP.parse_command(["XACK", "emails", "workers", "abc-123"]) ==
               {:xack, "emails", "abc-123"}
    end

    test "XNACK" do
      assert RESP.parse_command(["XNACK", "emails", "workers", "abc-123"]) ==
               {:xnack, "emails", "abc-123"}
    end

    test "XDEL parses to xdel_nack" do
      assert RESP.parse_command(["XDEL", "emails", "abc-123"]) ==
               {:xdel_nack, "emails", "abc-123"}
    end

    test "XDEL is case-insensitive" do
      assert {:xdel_nack, "emails", "abc-123"} =
               RESP.parse_command(["xdel", "emails", "abc-123"])
    end

    test "CLIENT SETNAME parses to client_setname" do
      assert RESP.parse_command(["CLIENT", "SETNAME", "my-worker"]) == {:client_setname}
    end

    test "CLIENT SETNAME is case-insensitive" do
      assert {:client_setname} = RESP.parse_command(["client", "setname", "x"])
    end

    test "XLEN" do
      assert RESP.parse_command(["XLEN", "emails"]) == {:xlen, "emails"}
    end

    test "XINFO STREAM" do
      assert RESP.parse_command(["XINFO", "STREAM", "emails"]) == {:xinfo_stream, "emails"}
    end

    test "unknown command returns {:unknown, tokens}" do
      assert {:unknown, ["PING"]} = RESP.parse_command(["PING"])
      assert {:unknown, ["GET", "key"]} = RESP.parse_command(["GET", "key"])
    end
  end

  # ---------------------------------------------------------------------------
  # Response encoders
  # ---------------------------------------------------------------------------

  describe "response encoders" do
    test "encode_push_response returns bulk string id" do
      wire = IO.iodata_to_binary(RESP.encode_push_response("1234567890-0"))
      assert wire == "$12\r\n1234567890-0\r\n"
    end

    test "encode_pop_response nil task returns stream with empty entries" do
      wire = IO.iodata_to_binary(RESP.encode_pop_response("emails", nil))
      # [[stream_name, []]]
      assert {:ok, [["emails", []]], ""} = RESP.decode(wire)
    end

    test "encode_pop_response with task returns nested stream format" do
      task = %{
        id: "1234567890-0",
        payload: "hello",
        attempts: 1,
        max_attempts: 3
      }

      wire = IO.iodata_to_binary(RESP.encode_pop_response("emails", task))
      {:ok, decoded, ""} = RESP.decode(wire)

      # [[stream_name, [[id, [field, val, ...]]]]]
      assert [["emails", [["1234567890-0", fields]]]] = decoded
      assert Enum.find_index(fields, &(&1 == "payload")) != nil
      assert Enum.at(fields, Enum.find_index(fields, &(&1 == "payload")) + 1) == "hello"
    end

    test "encode_block_timeout returns null bulk string" do
      assert IO.iodata_to_binary(RESP.encode_block_timeout()) == "$-1\r\n"
    end

    test "encode_xack_response :ok → 1" do
      wire = IO.iodata_to_binary(RESP.encode_xack_response(:ok))
      assert RESP.decode(wire) == {:ok, 1, ""}
    end

    test "encode_xack_response :not_found → 0" do
      wire = IO.iodata_to_binary(RESP.encode_xack_response(:not_found))
      assert RESP.decode(wire) == {:ok, 0, ""}
    end

    test "encode_xlen_response" do
      wire = IO.iodata_to_binary(RESP.encode_xlen_response(7))
      assert RESP.decode(wire) == {:ok, 7, ""}
    end

    test "encode_xinfo_response" do
      wire = IO.iodata_to_binary(RESP.encode_xinfo_response(%{queue: "emails", length: 5, dead: 1}))
      {:ok, decoded, ""} = RESP.decode(wire)
      assert is_list(decoded)
      idx = Enum.find_index(decoded, &(&1 == "name"))
      assert Enum.at(decoded, idx + 1) == "emails"
    end

    test "encode_error" do
      wire = IO.iodata_to_binary(RESP.encode_error("WRONGTYPE bad call"))
      assert RESP.decode(wire) == {:ok, {:error, "WRONGTYPE bad call"}, ""}
    end

    test "encode_ok" do
      wire = IO.iodata_to_binary(RESP.encode_ok())
      assert RESP.decode(wire) == {:ok, {:simple, "OK"}, ""}
    end
  end
end
