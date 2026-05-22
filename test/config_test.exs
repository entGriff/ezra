defmodule Ezra.ConfigTest do
  use ExUnit.Case, async: true

  alias Ezra.Config

  describe "new!/1" do
    test "returns a valid config with required opts" do
      config = Config.new!(data_dir: "/tmp/ezra_test", name: :my_queue)
      assert config.data_dir == "/tmp/ezra_test"
      assert config.name == :my_queue
      assert config.db_path == "/tmp/ezra_test/ezra.db"
      assert config.port == nil
      assert config.scheduler_ms == 5_000
    end

    test "uses Ezra as default name" do
      config = Config.new!(data_dir: "/tmp/x")
      assert config.name == Ezra
    end

    test "raises when :data_dir is missing" do
      assert_raise KeyError, fn -> Config.new!(name: :q) end
    end

    test "raises when :data_dir is empty string" do
      assert_raise ArgumentError, ~r/:data_dir/, fn ->
        Config.new!(data_dir: "", name: :q)
      end
    end

    test "raises when :data_dir is not a string" do
      assert_raise ArgumentError, ~r/:data_dir/, fn ->
        Config.new!(data_dir: 123, name: :q)
      end
    end

    test "accepts valid :port" do
      config = Config.new!(data_dir: "/tmp/x", port: 6380)
      assert config.port == 6380
    end

    test "raises when :port is zero" do
      assert_raise ArgumentError, ~r/:port/, fn ->
        Config.new!(data_dir: "/tmp/x", port: 0)
      end
    end

    test "raises when :port is above 65535" do
      assert_raise ArgumentError, ~r/:port/, fn ->
        Config.new!(data_dir: "/tmp/x", port: 99_999)
      end
    end

    test "raises when :port is not an integer" do
      assert_raise ArgumentError, ~r/:port/, fn ->
        Config.new!(data_dir: "/tmp/x", port: "6380")
      end
    end

    test "accepts custom :scheduler_ms" do
      config = Config.new!(data_dir: "/tmp/x", scheduler_ms: 1_000)
      assert config.scheduler_ms == 1_000
    end

    test "raises when :scheduler_ms is zero" do
      assert_raise ArgumentError, ~r/:scheduler_ms/, fn ->
        Config.new!(data_dir: "/tmp/x", scheduler_ms: 0)
      end
    end

    test "raises when :scheduler_ms is negative" do
      assert_raise ArgumentError, ~r/:scheduler_ms/, fn ->
        Config.new!(data_dir: "/tmp/x", scheduler_ms: -1)
      end
    end

    test "db_path is data_dir joined with ezra.db" do
      config = Config.new!(data_dir: "/some/path")
      assert config.db_path == "/some/path/ezra.db"
    end
  end
end
