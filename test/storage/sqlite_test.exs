defmodule Ezra.Storage.SQLiteTest do
  use Ezra.DBCase, async: true

  describe "startup" do
    test "opens the database file", %{db_path: path} do
      assert File.exists?(path)
    end

    test "WAL mode is active", %{db: db} do
      [[mode]] = Ezra.Storage.SQLite.query!(db, "PRAGMA journal_mode", [])
      assert mode == "wal"
    end

    test "foreign keys are enforced", %{db: db} do
      [[enabled]] = Ezra.Storage.SQLite.query!(db, "PRAGMA foreign_keys", [])
      assert enabled == 1
    end

    test "schema_migrations table exists after boot", %{db: db} do
      rows =
        Ezra.Storage.SQLite.query!(
          db,
          "SELECT name FROM sqlite_master WHERE type='table' AND name='schema_migrations'",
          []
        )

      assert rows == [["schema_migrations"]]
    end

    test "all expected tables exist", %{db: db} do
      rows =
        Ezra.Storage.SQLite.query!(
          db,
          "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name",
          []
        )

      table_names = Enum.map(rows, fn [name] -> name end)
      assert "queues" in table_names
      assert "tasks" in table_names
      assert "schema_migrations" in table_names
    end

    test "all migrations are recorded", %{db: db} do
      rows =
        Ezra.Storage.SQLite.query!(
          db,
          "SELECT version FROM schema_migrations ORDER BY version",
          []
        )

      versions = Enum.map(rows, fn [v] -> v end)
      assert versions == [1, 2, 3, 4, 5]
    end
  end

  describe "migrations idempotency" do
    test "running migrations twice does not duplicate records", %{db: db} do
      conn = Ezra.Storage.SQLite.conn(db)
      :ok = Ezra.Storage.Migrations.run(conn)
      :ok = Ezra.Storage.Migrations.run(conn)

      rows =
        Ezra.Storage.SQLite.query!(
          db,
          "SELECT COUNT(*) FROM schema_migrations",
          []
        )

      [[count]] = rows
      assert count == 5
    end
  end

  describe "query!/3" do
    test "inserts and retrieves a row", %{db: db} do
      now = System.system_time(:microsecond)

      Ezra.Storage.SQLite.query!(
        db,
        "INSERT INTO queues (name, created_at) VALUES (?1, ?2)",
        ["test_queue", now]
      )

      rows =
        Ezra.Storage.SQLite.query!(
          db,
          "SELECT name FROM queues WHERE name = ?1",
          ["test_queue"]
        )

      assert rows == [["test_queue"]]
    end

    test "returns an empty list when no rows match", %{db: db} do
      rows =
        Ezra.Storage.SQLite.query!(db, "SELECT * FROM queues WHERE name = ?1", ["missing"])

      assert rows == []
    end

    test "multiple rows are returned in insertion order", %{db: db} do
      now = System.system_time(:microsecond)

      for name <- ["alpha", "beta", "gamma"] do
        Ezra.Storage.SQLite.query!(
          db,
          "INSERT INTO queues (name, created_at) VALUES (?1, ?2)",
          [name, now]
        )
      end

      rows =
        Ezra.Storage.SQLite.query!(
          db,
          "SELECT name FROM queues ORDER BY name",
          []
        )

      assert rows == [["alpha"], ["beta"], ["gamma"]]
    end
  end

  describe "data persistence" do
    test "data written in one process is readable in another", %{db_path: path} do
      now = System.system_time(:microsecond)
      uid = System.unique_integer([:positive])

      {:ok, db1} =
        start_supervised({Ezra.Storage.SQLite, path: path, name: :"db1_#{uid}"}, id: :db1)

      Ezra.Storage.SQLite.query!(
        db1,
        "INSERT INTO queues (name, created_at) VALUES (?1, ?2)",
        ["persistent_queue", now]
      )

      # Stop db1, open a fresh connection to the same file
      :ok = stop_supervised(:db1)

      {:ok, db2} =
        start_supervised({Ezra.Storage.SQLite, path: path, name: :"db2_#{uid}"}, id: :db2)

      rows =
        Ezra.Storage.SQLite.query!(
          db2,
          "SELECT name FROM queues WHERE name = ?1",
          ["persistent_queue"]
        )

      assert rows == [["persistent_queue"]]
    end
  end
end
