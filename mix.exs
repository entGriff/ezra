defmodule Ezra.MixProject do
  use Mix.Project

  @version "0.1.0"

  def project do
    [
      app: :ezra,
      version: @version,
      elixir: "~> 1.17",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      test_coverage: [tool: ExCoveralls],
      preferred_cli_env: [
        coveralls: :test,
        "coveralls.html": :test
      ],
      releases: releases()
    ]
  end

  def application do
    [
      extra_applications: [:logger, :crypto],
      mod: {Ezra.Application, []}
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp deps do
    [
      # SQLite NIF wrapper (C amalgamation, no Rust required)
      {:exqlite, "~> 0.23"},

      # TCP server (used by Phoenix; battle-tested)
      {:ranch, "~> 2.2"},

      # Instrumentation
      {:telemetry, "~> 1.2"},

      # Single-binary packaging - build only, not started at runtime
      {:burrito, github: "burrito-elixir/burrito", tag: "v1.3.0", runtime: false},

      # Test tooling
      {:excoveralls, "~> 0.18", only: :test}
    ]
  end

  defp releases do
    [
      ezra: [
        include_executables_for: [:unix],
        strip_beams: true,
        # Set BURRITO_WRAP=true to produce a self-contained binary via Burrito.
        # Without it, `mix release` produces a standard OTP release.
        steps: release_steps(),
        burrito: [targets: burrito_targets()]
      ]
    ]
  end

  defp release_steps do
    if System.get_env("BURRITO_WRAP") == "true" do
      [:assemble, &Burrito.wrap/1]
    else
      [:assemble]
    end
  end

  defp burrito_targets do
    case System.get_env("BURRITO_TARGET") do
      nil ->
        [
          linux_x86_64: [os: :linux, cpu: :x86_64],
          linux_arm64:  [os: :linux, cpu: :aarch64],
          macos_arm64:  [os: :darwin, cpu: :aarch64]
        ]

      target ->
        [{String.to_atom(target), target_opts(target)}]
    end
  end

  # NOTE: Burrito uses `:darwin` (not `:macos`) for the OS atom internally.
  # `make_triplet/1`, `is_cross_build?/1`, and the pre-compiled ERTS tuple list
  # all pattern-match on `:darwin`.  The target *alias* (`:macos_arm64`) can be
  # any name we want - it only affects the artifact filename.
  defp target_opts("linux_x86_64"), do: build_target_opts(:linux, :x86_64)
  defp target_opts("linux_arm64"),  do: build_target_opts(:linux, :aarch64)
  defp target_opts("macos_arm64"),  do: build_target_opts(:darwin, :aarch64)

  # custom_erts: use the locally installed OTP from erlef/setup-beam in CI
  # rather than Burrito's stale v1.3.0 download URLs.
  #
  # skip_nifs: true - Burrito forces cross_build=true on Linux, which tries to
  # recompile NIFs. erlef/setup-beam strips the OTP root so the include path
  # resolves to nil and the build fails. The NIF is already compiled by mix
  # compile on the same machine, so skipping is fine.
  defp build_target_opts(os, cpu) do
    base = [os: os, cpu: cpu]

    base =
      case System.get_env("INSTALL_DIR_FOR_OTP") do
        nil -> base
        path -> [custom_erts: path] ++ base
      end

    if os == :linux, do: [skip_nifs: true] ++ base, else: base
  end
end
