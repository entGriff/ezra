defmodule Ezra.CLI do
  @moduledoc false

  # Command-line interface for standalone binary mode.
  #
  # parse/1 is a pure function - no side effects, no System.halt.
  # The Application calls it and decides what to do with the result.
  #
  # Precedence for every option: CLI flag > environment variable > default.

  @version Mix.Project.config()[:version]

  @spec parse([String.t()]) ::
          {:ok, keyword()}
          | :help
          | :version
          | {:error, String.t()}
  def parse(argv) when is_list(argv) do
    {parsed, _args, invalid} =
      OptionParser.parse(argv,
        strict: [
          data_dir: :string,
          port: :integer,
          host: :string,
          visibility_timeout: :integer,
          max_attempts: :integer,
          retention_seconds: :integer,
          scheduler_ms: :integer,
          help: :boolean,
          version: :boolean
        ],
        aliases: [d: :data_dir, p: :port, h: :help, v: :version]
      )

    cond do
      invalid != [] ->
        [{flag, _} | _] = invalid
        {:error, "unknown option: #{flag}"}

      Keyword.get(parsed, :help) ->
        :help

      Keyword.get(parsed, :version) ->
        :version

      true ->
        data_dir = Keyword.get(parsed, :data_dir) || System.get_env("EZRA_DATA_DIR")

        if is_nil(data_dir) or data_dir == "" do
          {:error, "--data-dir is required (or set EZRA_DATA_DIR)\n\nRun `ezra --help` for usage."}
        else
          {:ok, Keyword.merge(parsed, [name: Ezra, data_dir: data_dir])}
        end
    end
  end

  @spec version() :: String.t()
  def version, do: @version

  @spec help_text() :: String.t()
  def help_text do
    """
    ezra #{@version} - single-file persistent task queue

    Usage:
      ezra --data-dir PATH [options]

    Options:
      -d, --data-dir PATH          Directory to store ezra.db         (required, or $EZRA_DATA_DIR)
      -p, --port PORT              TCP port for RESP server            (default: 42002, or $EZRA_PORT)
          --host HOST              Bind address                        (default: 0.0.0.0, or $EZRA_HOST)
          --visibility-timeout N   Seconds before stuck task requeues  (default: 30, or $EZRA_VISIBILITY_TIMEOUT)
          --max-attempts N         Retry limit before task goes dead   (default: 3, or $EZRA_MAX_ATTEMPTS)
          --retention-seconds N    Delete tasks older than N seconds   (default: off, or $EZRA_RETENTION_SECONDS)
          --scheduler-ms N         Sweep interval in milliseconds      (default: 5000, or $EZRA_SCHEDULER_MS)
      -h, --help                   Print this message
      -v, --version                Print version

    Examples:
      ezra --data-dir /var/lib/ezra
      ezra --data-dir /var/lib/ezra --port 42002 --host 127.0.0.1
      EZRA_DATA_DIR=/var/lib/ezra EZRA_PORT=42002 ezra
    """
  end
end
