defmodule Ezra.Storage.SQLite do
  @moduledoc """
  GenServer that owns the SQLite database connection for one EZRA instance.

  Responsibilities:
  - Open the database file on startup and set WAL pragmas.
  - Run migrations before accepting any queries.
  - Provide `query!/3` for parameterised SQL execution.
  - Close the connection cleanly on shutdown.

  All writes to the database go through `Ezra.Queue.Engine`, which serialises
  them. This GenServer is used directly only for read-only queries and from
  the Engine itself.
  """

  use GenServer
  require Logger

  @pragmas [
    "PRAGMA journal_mode = WAL",
    "PRAGMA synchronous = NORMAL",
    "PRAGMA foreign_keys = ON",
    "PRAGMA busy_timeout = 5000",
    # 64 MB page cache. Default is ~2 MB, which means the available-tasks
    # index gets evicted as soon as the queue grows to a few thousand rows.
    # 64 MB keeps the hot index in memory at essentially zero cost on any
    # server that can run EZRA.
    "PRAGMA cache_size = -65536"
  ]

  # --- Public API ---

  def start_link(opts) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Executes a parameterised SQL statement and returns all result rows.

  Raises on any SQLite error.
  """
  @spec query!(GenServer.server(), String.t(), list()) :: [list()]
  def query!(server \\ __MODULE__, sql, params \\ []) do
    GenServer.call(server, {:query, sql, params})
  end

  @doc """
  Returns the raw `Exqlite.Sqlite3` connection handle.

  Use only when you need to run multiple statements in a single transaction
  without going through `query!/3` for each one.
  """
  @spec conn(GenServer.server()) :: Exqlite.Sqlite3.db()
  def conn(server \\ __MODULE__) do
    GenServer.call(server, :conn)
  end

  # --- GenServer callbacks ---

  @impl GenServer
  def init(opts) do
    path = Keyword.fetch!(opts, :path)
    Logger.info("[Ezra.Storage.SQLite] opening #{path}")

    {:ok, conn} = Exqlite.Sqlite3.open(path)

    Enum.each(@pragmas, fn pragma ->
      :ok = Exqlite.Sqlite3.execute(conn, pragma)
    end)

    :ok = Ezra.Storage.Migrations.run(conn)

    {:ok, %{conn: conn, path: path}}
  end

  @impl GenServer
  def handle_call({:query, sql, params}, _from, %{conn: conn} = state) do
    result = execute_query(conn, sql, params)
    {:reply, result, state}
  end

  def handle_call(:conn, _from, %{conn: conn} = state) do
    {:reply, conn, state}
  end

  @impl GenServer
  def terminate(_reason, %{conn: conn}) do
    Exqlite.Sqlite3.close(conn)
  end

  # --- Private ---

  defp execute_query(conn, sql, params) do
    {:ok, stmt} = Exqlite.Sqlite3.prepare(conn, sql)

    if params != [] do
      :ok = Exqlite.Sqlite3.bind(stmt, params)
    end

    rows = collect_rows(conn, stmt)
    :ok = Exqlite.Sqlite3.release(conn, stmt)
    rows
  end

  defp collect_rows(conn, stmt, acc \\ []) do
    case Exqlite.Sqlite3.step(conn, stmt) do
      {:row, row} -> collect_rows(conn, stmt, [row | acc])
      :done -> Enum.reverse(acc)
    end
  end
end
