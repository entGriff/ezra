defmodule Ezra.Supervisor do
  @moduledoc """
  Top-level supervisor for one EZRA instance.

  Starts children in order - storage first, then the engine (which depends on
  storage), then the scheduler. The TCP server is added only when a port is
  configured.

  Each child is namespaced under `config.name` so multiple independent EZRA
  instances can run in the same BEAM without colliding.
  """

  use Supervisor

  alias Ezra.Config

  def start_link(opts) do
    config = Config.new!(opts)
    Supervisor.start_link(__MODULE__, config, name: :"#{config.name}.Supervisor")
  end

  @impl Supervisor
  def init(%Config{} = config) do
    :ok = File.mkdir_p!(config.data_dir)

    children =
      [
        {Ezra.Storage.SQLite,
         path: config.db_path, name: db_name(config)},

        {Ezra.Queue.Engine,
         db: db_name(config), name: engine_name(config),
         default_visibility_timeout: config.visibility_timeout,
         default_max_attempts: config.max_attempts,
         default_retention_seconds: config.retention_seconds},

        {Ezra.Queue.Scheduler,
         db: db_name(config), interval_ms: config.scheduler_ms,
         name: scheduler_name(config)}
      ]
      |> maybe_add_server(config)

    Supervisor.init(children, strategy: :one_for_one)
  end

  # --- Child name helpers ---

  def db_name(%Config{name: n}), do: :"#{n}.Storage.SQLite"
  def engine_name(%Config{name: n}), do: :"#{n}.Queue.Engine"
  def scheduler_name(%Config{name: n}), do: :"#{n}.Queue.Scheduler"

  # --- Private ---

  defp maybe_add_server(children, %Config{port: nil}), do: children

  defp maybe_add_server(children, %Config{port: port} = config) do
    server = {Ezra.Server.Supervisor,
              name: config.name,
              port: port,
              host: config.host,
              engine: engine_name(config)}

    children ++ [server]
  end
end
