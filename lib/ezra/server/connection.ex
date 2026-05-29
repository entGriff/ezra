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
      buf: <<>>
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
        response = dispatch(RESP.parse_command(tokens), state)
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

  defp dispatch({:client_setname}, _state) do
    RESP.encode_ok()
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
        RESP.encode_pop_response(queue, task)

      {:empty} when block_ms > 0 ->
        RESP.encode_block_timeout()

      {:empty} ->
        RESP.encode_pop_response(queue, nil)
    end
  end

  defp dispatch({:xack, _queue, id_str}, state) do
    case Engine.ack(state.engine, String.to_integer(id_str)) do
      :ok -> RESP.encode_xack_response(:ok)
      {:error, :not_found} -> RESP.encode_xack_response(:not_found)
    end
  end

  defp dispatch({:xnack, _queue, id_str}, state) do
    case Engine.nack(state.engine, String.to_integer(id_str)) do
      {:ok, _} -> RESP.encode_ok()
      {:error, :not_found} -> RESP.encode_error("ERR task not found")
    end
  end

  defp dispatch({:xdel_nack, _queue, id_str}, state) do
    case Engine.nack(state.engine, String.to_integer(id_str)) do
      {:ok, _} -> RESP.encode(1)
      {:error, :not_found} -> RESP.encode(0)
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

  defp fields_to_push_opts(fields) do
    for {key, fn_convert} <- [{"ttl", &{:ttl_seconds, String.to_integer(&1)}},
                               {"max_attempts", &{:max_attempts, String.to_integer(&1)}}],
        val = fields[key],
        val != nil,
        do: fn_convert.(val)
  end
end
