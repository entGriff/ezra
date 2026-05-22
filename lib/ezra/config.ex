defmodule Ezra.Config do
  @moduledoc """
  Validated configuration for one EZRA instance.

  Values are resolved in priority order: explicit opts > environment variables > defaults.

  ## Environment variables

  | Variable                  | Default | Description                              |
  |---------------------------|---------|------------------------------------------|
  | `EZRA_PORT`               | nil     | TCP port for the RESP server             |
  | `EZRA_HOST`               | 0.0.0.0 | Address to bind to                       |
  | `EZRA_SCHEDULER_MS`       | 5000    | Sweep interval in milliseconds           |
  | `EZRA_VISIBILITY_TIMEOUT` | 30      | Seconds before a stuck task is requeued  |
  | `EZRA_MAX_ATTEMPTS`       | 3       | Times a task is retried before going dead|
  | `EZRA_RETENTION_SECONDS`  | nil     | Delete tasks older than N seconds        |
  """

  @enforce_keys [:name, :data_dir, :db_path]

  defstruct [
    :name,
    :data_dir,
    :db_path,
    port: nil,
    host: "0.0.0.0",
    scheduler_ms: 5_000,
    visibility_timeout: 30,
    max_attempts: 3,
    retention_seconds: nil
  ]

  @type t :: %__MODULE__{
          name: atom(),
          data_dir: String.t(),
          db_path: String.t(),
          port: pos_integer() | nil,
          host: String.t(),
          scheduler_ms: pos_integer(),
          visibility_timeout: pos_integer(),
          max_attempts: pos_integer(),
          retention_seconds: pos_integer() | nil
        }

  @doc """
  Parses and validates options, returning a Config struct.
  Raises `ArgumentError` on invalid options.
  """
  @spec new!(keyword()) :: t()
  def new!(opts) do
    data_dir = Keyword.fetch!(opts, :data_dir)
    name = Keyword.get(opts, :name, Ezra)

    unless is_binary(data_dir) and data_dir != "" do
      raise ArgumentError, "Ezra: :data_dir must be a non-empty string"
    end

    port = Keyword.get(opts, :port, env_int("EZRA_PORT"))

    if port != nil and (not is_integer(port) or port < 1 or port > 65_535) do
      raise ArgumentError, "Ezra: :port must be an integer between 1 and 65535"
    end

    host = Keyword.get(opts, :host, env_str("EZRA_HOST", "0.0.0.0"))

    unless is_binary(host) and host != "" do
      raise ArgumentError, "Ezra: :host must be a non-empty string (e.g. \"0.0.0.0\")"
    end

    scheduler_ms = Keyword.get(opts, :scheduler_ms, env_int("EZRA_SCHEDULER_MS", 5_000))

    unless is_integer(scheduler_ms) and scheduler_ms > 0 do
      raise ArgumentError, "Ezra: :scheduler_ms must be a positive integer"
    end

    visibility_timeout = Keyword.get(opts, :visibility_timeout, env_int("EZRA_VISIBILITY_TIMEOUT", 30))

    unless is_integer(visibility_timeout) and visibility_timeout > 0 do
      raise ArgumentError, "Ezra: :visibility_timeout must be a positive integer (seconds)"
    end

    max_attempts = Keyword.get(opts, :max_attempts, env_int("EZRA_MAX_ATTEMPTS", 3))

    unless is_integer(max_attempts) and max_attempts >= 1 do
      raise ArgumentError, "Ezra: :max_attempts must be an integer >= 1"
    end

    retention_seconds = Keyword.get(opts, :retention_seconds, env_int("EZRA_RETENTION_SECONDS"))

    if retention_seconds != nil and (not is_integer(retention_seconds) or retention_seconds < 1) do
      raise ArgumentError, "Ezra: :retention_seconds must be a positive integer or nil"
    end

    %__MODULE__{
      name: name,
      data_dir: data_dir,
      db_path: Path.join(data_dir, "ezra.db"),
      port: port,
      host: host,
      scheduler_ms: scheduler_ms,
      visibility_timeout: visibility_timeout,
      max_attempts: max_attempts,
      retention_seconds: retention_seconds
    }
  end

  # --- Env var helpers ---

  defp env_int(key, default \\ nil) do
    case System.get_env(key) do
      nil -> default
      val ->
        case Integer.parse(val) do
          {n, ""} -> n
          _ -> default
        end
    end
  end

  defp env_str(key, default) do
    case System.get_env(key) do
      nil -> default
      "" -> default
      val -> val
    end
  end
end
