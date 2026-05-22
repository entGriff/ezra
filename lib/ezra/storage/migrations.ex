defmodule Ezra.Storage.Migrations do
  @moduledoc """
  Versioned, append-only SQLite migrations.

  Each migration is a `{version, sql}` tuple. Migrations are applied in order
  and never re-applied. Adding a migration means appending to `@migrations` -
  never editing an existing entry.
  """

  @migrations [
    {1,
     """
     CREATE TABLE IF NOT EXISTS schema_migrations (
       version    INTEGER NOT NULL PRIMARY KEY,
       applied_at INTEGER NOT NULL
     );
     """},
    {2,
     """
     CREATE TABLE IF NOT EXISTS queues (
       name                TEXT    NOT NULL PRIMARY KEY,
       max_attempts        INTEGER NOT NULL DEFAULT 3,
       visibility_timeout  INTEGER NOT NULL DEFAULT 30,
       retention_seconds   INTEGER,
       created_at          INTEGER NOT NULL
     );
     """},
    {3,
     """
     CREATE TABLE IF NOT EXISTS tasks (
       id                  INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
       queue               TEXT    NOT NULL REFERENCES queues(name),
       payload             BLOB    NOT NULL,
       status              TEXT    NOT NULL DEFAULT 'available',
       attempts            INTEGER NOT NULL DEFAULT 0,
       max_attempts        INTEGER NOT NULL DEFAULT 3,
       inserted_at         INTEGER NOT NULL,
       scheduled_at        INTEGER NOT NULL,
       claimed_at          INTEGER,
       worker_id           TEXT,
       visibility_timeout  INTEGER NOT NULL DEFAULT 30,
       expires_at          INTEGER,
       last_error          TEXT
     );
     """},
    {4,
     """
     CREATE INDEX IF NOT EXISTS idx_tasks_available
       ON tasks (queue, scheduled_at)
       WHERE status = 'available';
     """},
    {5,
     """
     CREATE INDEX IF NOT EXISTS idx_tasks_in_flight
       ON tasks (claimed_at)
       WHERE status = 'in_flight';
     """}
  ]

  @doc """
  Runs all pending migrations against the given database connection.
  Safe to call on every startup - already-applied migrations are skipped.
  """
  def run(conn) do
    ensure_migrations_table(conn)
    applied = applied_versions(conn)

    @migrations
    |> Enum.reject(fn {version, _sql} -> version in applied end)
    |> Enum.each(fn {version, sql} -> apply_migration(conn, version, sql) end)

    :ok
  end

  defp ensure_migrations_table(conn) do
    sql = """
    CREATE TABLE IF NOT EXISTS schema_migrations (
      version    INTEGER NOT NULL PRIMARY KEY,
      applied_at INTEGER NOT NULL
    );
    """

    :ok = Exqlite.Sqlite3.execute(conn, sql)
  end

  defp applied_versions(conn) do
    {:ok, statement} =
      Exqlite.Sqlite3.prepare(conn, "SELECT version FROM schema_migrations ORDER BY version")

    rows = collect_rows(conn, statement)
    :ok = Exqlite.Sqlite3.release(conn, statement)
    Enum.map(rows, fn [version] -> version end)
  end

  defp apply_migration(conn, version, sql) do
    :ok = Exqlite.Sqlite3.execute(conn, sql)

    {:ok, stmt} =
      Exqlite.Sqlite3.prepare(
        conn,
        "INSERT INTO schema_migrations (version, applied_at) VALUES (?1, ?2)"
      )

    :ok = Exqlite.Sqlite3.bind(stmt, [version, now_us()])
    :done = Exqlite.Sqlite3.step(conn, stmt)
    :ok = Exqlite.Sqlite3.release(conn, stmt)
  end

  defp collect_rows(conn, statement) do
    collect_rows(conn, statement, [])
  end

  defp collect_rows(conn, statement, acc) do
    case Exqlite.Sqlite3.step(conn, statement) do
      {:row, row} -> collect_rows(conn, statement, [row | acc])
      :done -> Enum.reverse(acc)
    end
  end

  defp now_us, do: System.system_time(:microsecond)
end
