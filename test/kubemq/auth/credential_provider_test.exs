defmodule KubeMQ.Auth.CredentialProviderTest do
  use ExUnit.Case, async: true

  describe "behaviour compliance" do
    test "module implementing @behaviour compiles and works" do
      defmodule TestProvider do
        @behaviour KubeMQ.CredentialProvider
        @impl true
        def get_token(_opts), do: {:ok, "test-token"}
      end

      assert {:ok, "test-token"} = TestProvider.get_token([])
    end

    test "mock returning ok" do
      provider = fn _opts -> {:ok, "dynamic-token"} end
      assert {:ok, "dynamic-token"} = provider.([])
    end

    test "mock returning error" do
      provider = fn _opts -> {:error, :auth_failed} end
      assert {:error, :auth_failed} = provider.([])
    end

    test "opts passthrough" do
      defmodule OptsProvider do
        @behaviour KubeMQ.CredentialProvider
        @impl true
        def get_token(opts), do: {:ok, Keyword.get(opts, :token, "default")}
      end

      assert {:ok, "custom"} = OptsProvider.get_token(token: "custom")
    end
  end
end
