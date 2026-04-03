defmodule KubeMQ.UUIDTest do
  use ExUnit.Case, async: true

  alias KubeMQ.UUID

  @uuid_v4_regex ~r/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/

  describe "generate/0" do
    test "returns a UUID v4 formatted string" do
      uuid = UUID.generate()
      assert Regex.match?(@uuid_v4_regex, uuid)
    end

    test "generates unique UUIDs across 100 calls" do
      uuids = for _ <- 1..100, do: UUID.generate()
      assert length(Enum.uniq(uuids)) == 100
    end
  end

  describe "ensure/1" do
    test "generates a UUID when given nil" do
      result = UUID.ensure(nil)
      assert is_binary(result)
      assert Regex.match?(@uuid_v4_regex, result)
    end

    test "generates a UUID when given empty string" do
      result = UUID.ensure("")
      assert is_binary(result)
      assert Regex.match?(@uuid_v4_regex, result)
    end

    test "returns the existing id when given a non-empty string" do
      existing = "my-custom-id"
      assert UUID.ensure(existing) == "my-custom-id"
    end
  end
end
