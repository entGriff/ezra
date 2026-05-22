defmodule Ezra.Application do
  @moduledoc false

  # In standalone binary mode (./ezra --port 42002 --data-dir /var/lib/ezra),
  # System.argv() contains the user's flags and we start the full supervisor.
  #
  # In library mode ({Ezra, opts} in a host supervision tree), System.argv()
  # has no --data-dir so we start an empty supervisor and let the host app
  # manage EZRA instances.

  use Application

  @impl Application
  def start(_type, _args) do
    case Ezra.CLI.parse(System.argv()) do
      {:ok, opts} ->
        start_standalone(opts)

      :help ->
        IO.puts(Ezra.CLI.help_text())
        System.halt(0)

      :version ->
        IO.puts(Ezra.CLI.version())
        System.halt(0)

      {:error, _msg} ->
        # Library mode - no --data-dir in argv; host app starts {Ezra, opts}
        Supervisor.start_link([], strategy: :one_for_one, name: __MODULE__)
    end
  end

  defp start_standalone(opts) do
    IO.puts("[ezra] port=#{opts[:port]} data_dir=#{opts[:data_dir]} host=#{opts[:host]}")
    children = [{Ezra, opts}]
    Supervisor.start_link(children, strategy: :one_for_one, name: __MODULE__)
  end
end
