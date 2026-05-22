defmodule Ezra.Server.Supervisor do
  @moduledoc false

  # Wraps the Ranch TCP listener as an OTP supervisor child.
  # Only started when the EZRA instance is configured with a port.
  #
  # Ranch manages its own acceptor pool and per-connection process tree
  # underneath this supervisor, so we simply hand it a child spec.

  use Supervisor

  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    Supervisor.start_link(__MODULE__, opts, name: :"#{name}.Server.Supervisor")
  end

  @impl Supervisor
  def init(opts) do
    port = Keyword.fetch!(opts, :port)
    engine = Keyword.fetch!(opts, :engine)
    name = Keyword.fetch!(opts, :name)
    host = Keyword.get(opts, :host, "0.0.0.0")

    # Unique Ranch ref per EZRA instance - allows multiple instances on
    # different ports in the same BEAM without ref collisions.
    ranch_ref = {__MODULE__, name}

    listener =
      :ranch.child_spec(
        ranch_ref,
        :ranch_tcp,
        %{socket_opts: [port: port, reuseaddr: true, ip: parse_host(host)]},
        Ezra.Server.Connection,
        %{engine: engine}
      )

    Supervisor.init([listener], strategy: :one_for_one)
  end

  defp parse_host(host) do
    case :inet.parse_address(String.to_charlist(host)) do
      {:ok, ip} -> ip
      {:error, _} -> raise ArgumentError, "Ezra: invalid :host address: #{inspect(host)}"
    end
  end
end
