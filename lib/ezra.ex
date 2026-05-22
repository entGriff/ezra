defmodule Ezra do
  @moduledoc """
  EZRA - Elixir Zero-config Relay Archive.

  A single-binary, single-file, language-agnostic persistent task queue.

  ## Embedded usage (Elixir library mode)

  Add to your supervision tree:

      children = [
        {Ezra, name: :ezra, data_dir: "priv/ezra"}
      ]

  Then use the API from anywhere in your application:

      {:ok, id}   = Ezra.push(:ezra, "emails", Jason.encode!(%{to: "alice@example.com"}))
      {:ok, task} = Ezra.pop(:ezra, "emails", worker_id: "w1", block: 30_000)
      :ok         = Ezra.ack(:ezra, task.id)

  ## Network mode

  Pass `port:` to also start a RESP-compatible TCP server:

      {Ezra, name: :ezra, data_dir: "priv/ezra", port: 42002}

  External workers connect using any Redis client library, pointing to that port.
  """

  alias Ezra.{Config, Supervisor, Queue.Engine}

  @doc """
  Returns a child specification for use in a supervision tree.

  Opts are the same as `start_link/1`.
  """
  def child_spec(opts) do
    %{
      id: Keyword.get(opts, :name, __MODULE__),
      start: {__MODULE__, :start_link, [opts]},
      type: :supervisor,
      restart: :permanent,
      shutdown: 5_000
    }
  end

  @doc """
  Starts an EZRA instance under the calling process.

  In practice you pass `{Ezra, opts}` to a supervision tree rather than
  calling this directly.
  """
  def start_link(opts) do
    Supervisor.start_link(opts)
  end

  # --- Public task API ---

  @doc """
  Pushes a task onto the named queue.

  `name` is the atom you gave when starting EZRA (e.g. `:ezra`).
  `queue` is any string (e.g. `"emails"`).
  `payload` is any binary - typically a JSON-encoded string.

  Options:
  - `:max_attempts`      - override the queue default (integer, >= 1)
  - `:visibility_timeout`- seconds before a stuck task is requeued (integer)
  - `:ttl_seconds`       - delete the task after this many seconds if unprocessed
  - `:scheduled_at`      - Unix microseconds; task won't be popped until then

  Returns `{:ok, task_id}`.
  """
  @spec push(atom(), String.t(), binary(), keyword()) :: {:ok, pos_integer()}
  def push(name, queue, payload, opts \\ []) do
    Engine.push(engine(name), queue, payload, opts)
  end

  @doc """
  Pops the oldest available task from the named queue.

  Options:
  - `:worker_id` - identifies this consumer (string, default `"unknown"`)
  - `:block`     - milliseconds to wait if the queue is empty (default `0`)

  Returns `{:ok, task}` or `{:empty}`.
  """
  @spec pop(atom(), String.t(), keyword()) ::
          {:ok, Ezra.Queue.Task.t()} | {:empty}
  def pop(name, queue, opts \\ []) do
    Engine.pop(engine(name), queue, opts)
  end

  @doc """
  Acknowledges that a task was processed successfully.

  Returns `:ok` or `{:error, :not_found}`.
  """
  @spec ack(atom(), pos_integer()) :: :ok | {:error, :not_found}
  def ack(name, task_id) do
    Engine.ack(engine(name), task_id)
  end

  @doc """
  Negative-acknowledges a task, returning it to the queue or moving it to dead.

  Options:
  - `:reason` - error message stored for debugging

  Returns `{:ok, :requeued}`, `{:ok, :dead}`, or `{:error, :not_found}`.
  """
  @spec nack(atom(), pos_integer(), keyword()) ::
          {:ok, :requeued | :dead} | {:error, :not_found}
  def nack(name, task_id, opts \\ []) do
    Engine.nack(engine(name), task_id, opts)
  end

  @doc """
  Returns statistics for the named queue.

      Ezra.queue_info(:ezra, "emails")
      #=> %{queue: "emails", available: 42, in_flight: 3, dead: 1}
  """
  @spec queue_info(atom(), String.t()) :: map()
  def queue_info(name, queue) do
    Engine.queue_info(engine(name), queue)
  end

  # --- Helpers ---

  defp engine(name) do
    Config.new!(name: name, data_dir: "")
    |> Supervisor.engine_name()
  rescue
    # engine/1 is only used to derive the registered name; data_dir validation
    # would fire but we never need to open a file here.
    _ -> :"#{name}.Queue.Engine"
  end
end
