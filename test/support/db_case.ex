defmodule Ezra.DBCase do
  @moduledoc """
  ExUnit case template for tests that need an isolated SQLite database.

  Each test gets a fresh, temporary database file that is deleted on exit.
  The SQLite GenServer is started as part of the test process tree so it
  terminates automatically when the test finishes.
  """

  use ExUnit.CaseTemplate

  using do
    quote do
      import Ezra.DBCase
    end
  end

  setup do
    db_path = temp_db_path()
    db_name = :"ezra_test_db_#{System.unique_integer([:positive])}"
    {:ok, pid} = start_supervised({Ezra.Storage.SQLite, path: db_path, name: db_name})
    on_exit(fn -> File.rm(db_path) end)
    {:ok, db: pid, db_path: db_path}
  end

  def temp_db_path do
    dir = System.tmp_dir!()
    name = "ezra_test_#{System.unique_integer([:positive])}.db"
    Path.join(dir, name)
  end
end
