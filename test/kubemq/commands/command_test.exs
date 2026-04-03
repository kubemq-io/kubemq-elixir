defmodule KubeMQ.CommandTest do
  use ExUnit.Case, async: true

  alias KubeMQ.Command

  describe "new/0" do
    test "returns struct with default values" do
      cmd = Command.new()
      assert %Command{} = cmd
      assert cmd.id == nil
      assert cmd.channel == nil
      assert cmd.metadata == nil
      assert cmd.body == nil
      assert cmd.timeout == 10_000
      assert cmd.client_id == nil
      assert cmd.tags == %{}
      assert cmd.span == nil
    end
  end

  describe "new/1" do
    test "sets all fields from keyword options" do
      cmd =
        Command.new(
          id: "cmd-1",
          channel: "orders.process",
          metadata: "meta",
          body: <<1, 2, 3>>,
          timeout: 5_000,
          client_id: "client-1",
          tags: %{"key" => "val"},
          span: <<9, 8>>
        )

      assert cmd.id == "cmd-1"
      assert cmd.channel == "orders.process"
      assert cmd.metadata == "meta"
      assert cmd.body == <<1, 2, 3>>
      assert cmd.timeout == 5_000
      assert cmd.client_id == "client-1"
      assert cmd.tags == %{"key" => "val"}
      assert cmd.span == <<9, 8>>
    end
  end
end
