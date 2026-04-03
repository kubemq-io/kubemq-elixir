defmodule KubeMQ.Auth.StaticTokenProviderTest do
  use ExUnit.Case, async: true

  alias KubeMQ.StaticTokenProvider

  describe "get_token/1" do
    test "returns {:ok, token} for a valid binary token" do
      assert {:ok, "my-jwt-token"} = StaticTokenProvider.get_token(auth_token: "my-jwt-token")
    end

    test "returns {:error, :no_token_configured} when auth_token is nil" do
      assert {:error, :no_token_configured} = StaticTokenProvider.get_token(auth_token: nil)
    end

    test "returns {:error, :empty_token} when auth_token is empty string" do
      assert {:error, :empty_token} = StaticTokenProvider.get_token(auth_token: "")
    end

    test "returns {:error, :invalid_token_type} for non-binary value" do
      assert {:error, :invalid_token_type} = StaticTokenProvider.get_token(auth_token: 12_345)
    end
  end
end
