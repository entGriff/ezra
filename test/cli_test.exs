defmodule Ezra.CLITest do
  use ExUnit.Case, async: true

  alias Ezra.CLI

  describe "parse/1" do
    test "returns {:ok, opts} with required --data-dir" do
      assert {:ok, opts} = CLI.parse(["--data-dir", "/tmp/ezra"])
      assert opts[:data_dir] == "/tmp/ezra"
      assert opts[:name] == Ezra
    end

    test "--port overrides default 6380" do
      assert {:ok, opts} = CLI.parse(["--data-dir", "/tmp/x", "--port", "7777"])
      assert opts[:port] == 7777
    end

    test "--host sets bind address" do
      assert {:ok, opts} = CLI.parse(["--data-dir", "/tmp/x", "--host", "127.0.0.1"])
      assert opts[:host] == "127.0.0.1"
    end

    test "short -d and -p aliases work" do
      assert {:ok, opts} = CLI.parse(["-d", "/tmp/x", "-p", "9999"])
      assert opts[:data_dir] == "/tmp/x"
      assert opts[:port] == 9999
    end

    test "--help returns :help" do
      assert :help = CLI.parse(["--help"])
    end

    test "-h returns :help" do
      assert :help = CLI.parse(["-h"])
    end

    test "--version returns :version" do
      assert :version = CLI.parse(["--version"])
    end

    test "-v returns :version" do
      assert :version = CLI.parse(["-v"])
    end

    test "empty argv returns error mentioning --data-dir" do
      assert {:error, msg} = CLI.parse([])
      assert String.contains?(msg, "--data-dir")
    end

    test "--port without --data-dir returns error" do
      assert {:error, msg} = CLI.parse(["--port", "6380"])
      assert String.contains?(msg, "--data-dir")
    end

    test "unknown flag returns error" do
      assert {:error, msg} = CLI.parse(["--data-dir", "/tmp/x", "--unknown"])
      assert String.contains?(msg, "unknown")
    end

    test "--help takes priority over missing --data-dir" do
      assert :help = CLI.parse(["--help"])
    end

    test "all flags together" do
      assert {:ok, opts} =
               CLI.parse([
                 "--data-dir", "/var/ezra",
                 "--port", "6380",
                 "--host", "0.0.0.0"
               ])

      assert opts[:data_dir] == "/var/ezra"
      assert opts[:port] == 6380
      assert opts[:host] == "0.0.0.0"
    end
  end

  describe "help_text/0" do
    test "contains usage line" do
      assert String.contains?(CLI.help_text(), "--data-dir")
    end

    test "contains --port description" do
      assert String.contains?(CLI.help_text(), "--port")
    end

    test "contains version" do
      assert String.contains?(CLI.help_text(), CLI.version())
    end
  end

  describe "version/0" do
    test "returns a version string" do
      assert is_binary(CLI.version())
      assert String.match?(CLI.version(), ~r/\d+\.\d+\.\d+/)
    end
  end
end
