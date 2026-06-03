defmodule Ezra.Server.Connection do
  @moduledoc false

  # Ranch protocol handler - one process per TCP connection.
  #
  # Accumulates bytes in a buffer, decodes RESP frames, dispatches each
  # command to the Engine, and writes the encoded response back.
  #
  # Blocking XREADGROUP (BLOCK ms > 0) parks the process inside
  # Engine.pop/3, which holds the GenServer.call open until a task arrives
  # or the timeout fires. The socket is not re-armed during the wait, so
  # no new client commands are processed mid-block. If the client
  # disconnects while blocked, {:tcp_closed, socket} sits in the mailbox
  # and is handled on the next loop iteration after pop returns.

  @behaviour :ranch_protocol

  alias Ezra.{Queue.Engine, Server.RESP}

  # --- Ranch entry point ---

  def start_link(ref, transport, opts) do
    pid = :proc_lib.spawn_link(__MODULE__, :init, [ref, transport, opts])
    {:ok, pid}
  end

  def init(ref, transport, opts) do
    {:ok, socket} = :ranch.handshake(ref)
    :ok = apply(transport, :setopts, [socket, [{:active, :once}]])

    loop(%{
      socket: socket,
      transport: transport,
      engine: opts.engine,
      buf: <<>>,
      proto: 2
    })
  end

  # --- Receive loop ---

  defp loop(%{socket: socket, transport: transport} = state) do
    receive do
      {proto, ^socket, data} when proto in [:tcp, :ssl] ->
        state = process_buffer(%{state | buf: state.buf <> data})
        :ok = apply(transport, :setopts, [socket, [{:active, :once}]])
        loop(state)

      {closed, ^socket} when closed in [:tcp_closed, :ssl_closed] ->
        apply(transport, :close, [socket])

      {err, ^socket, _reason} when err in [:tcp_error, :ssl_error] ->
        apply(transport, :close, [socket])
    end
  end

  # --- Buffer processing ---

  defp process_buffer(state) do
    case RESP.decode(state.buf) do
      {:ok, tokens, rest} when is_list(tokens) ->
        cmd   = RESP.parse_command(tokens)
        state = update_proto(state, cmd)
        response = dispatch(cmd, state)
        apply(state.transport, :send, [state.socket, IO.iodata_to_binary(response)])
        process_buffer(%{state | buf: rest})

      {:ok, _scalar, rest} ->
        process_buffer(%{state | buf: rest})

      {:more, _} ->
        state

      {:error, _} ->
        apply(state.transport, :send, [
          state.socket,
          IO.iodata_to_binary(RESP.encode_error("ERR protocol error"))
        ])
        %{state | buf: <<>>}
    end
  end

  # --- Command dispatch ---

  defp dispatch({:hello, v}, _state) do
    RESP.encode_hello(v)
  end

  defp dispatch({:ping, nil}, _state) do
    RESP.encode({:simple, "PONG"})
  end

  defp dispatch({:ping, msg}, _state) do
    RESP.encode(msg)
  end

  defp dispatch({:client_setname}, _state) do
    RESP.encode_ok()
  end

  defp dispatch({:info}, _state) do
    RESP.encode_info()
  end

  defp dispatch({:xadd, queue, fields}, state) do
    payload = Map.get(fields, "payload", "")

    {:ok, id} = Engine.push(state.engine, queue, payload, fields_to_push_opts(fields))
    RESP.encode_push_response(Integer.to_string(id))
  end

  defp dispatch({:xgroup_create, queue, _group}, state) do
    Engine.ensure_queue(state.engine, queue)
    RESP.encode_ok()
  end

  defp dispatch({:xreadgroup, _group, consumer, queue, _count, block_ms}, state) do
    case Engine.pop(state.engine, queue, worker_id: consumer, block: block_ms) do
      {:ok, task} ->
        task = %{task | id: Integer.to_string(task.id)}
        RESP.encode_pop_response(queue, task, state.proto)

      {:empty} when block_ms > 0 ->
        RESP.encode_block_timeout()

      {:empty} ->
        RESP.encode_pop_response(queue, nil, state.proto)
    end
  end

  defp dispatch({:xack, _queue, id_str}, state) do
    case parse_id(id_str) do
      {:ok, id} ->
        case Engine.ack(state.engine, id) do
          :ok -> RESP.encode_xack_response(:ok)
          {:error, :not_found} -> RESP.encode_xack_response(:not_found)
        end
      :error ->
        RESP.encode_error("ERR invalid task id")
    end
  end

  defp dispatch({:xnack, _queue, id_str}, state) do
    case parse_id(id_str) do
      {:ok, id} ->
        case Engine.nack(state.engine, id) do
          {:ok, _} -> RESP.encode_ok()
          {:error, :not_found} -> RESP.encode_error("ERR task not found")
        end
      :error ->
        RESP.encode_error("ERR invalid task id")
    end
  end

  defp dispatch({:xdel_nack, _queue, id_str}, state) do
    case parse_id(id_str) do
      {:ok, id} ->
        case Engine.nack(state.engine, id) do
          {:ok, _} -> RESP.encode(1)
          {:error, :not_found} -> RESP.encode(0)
        end
      :error ->
        RESP.encode(0)
    end
  end

  defp dispatch({:xlen, queue}, state) do
    %{available: n} = Engine.queue_info(state.engine, queue)
    RESP.encode_xlen_response(n)
  end

  defp dispatch({:xinfo_stream, queue}, state) do
    info = Engine.xinfo_stream(state.engine, queue)
    RESP.encode_xinfo_response(info)
  end

  defp dispatch({:unknown, []}, _state) do
    RESP.encode_error("ERR unknown command")
  end

  defp dispatch({:unknown, [cmd | _]}, _state) do
    RESP.encode_error("ERR unknown command '#{String.downcase(cmd)}'")
  end

  # --- Helpers ---

  # Store the negotiated protocol version so XREADGROUP can pick the right
  # response encoding.
  defp update_proto(state, {:hello, v}), do: %{state | proto: v}
  defp update_proto(state, _), do: state

  defp parse_id(str) do
    case Integer.parse(str) do
      {id, ""} when id > 0 -> {:ok, id}
      _ -> :error
    end
  end

  defp fields_to_push_opts(fields) do
    for {key, tag} <- [{"ttl", :ttl_seconds}, {"max_attempts", :max_attempts}],
        val = fields[key],
        val != nil,
        {n, ""} <- [Integer.parse(val)],
        n > 0,
        do: {tag, n}
  end
end
