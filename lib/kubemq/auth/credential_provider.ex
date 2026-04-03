defmodule KubeMQ.CredentialProvider do
  @moduledoc """
  Behaviour for providing authentication credentials to KubeMQ.

  Implement this behaviour to supply dynamic tokens (e.g., rotating OIDC tokens).
  For static tokens, use `KubeMQ.StaticTokenProvider` or the `:auth_token` client option.

  ## Example

      defmodule MyTokenProvider do
        @behaviour KubeMQ.CredentialProvider

        @impl true
        def get_token(_opts) do
          case MyVault.fetch_token("kubemq") do
            {:ok, token} -> {:ok, token}
            {:error, reason} -> {:error, reason}
          end
        end
      end

      {:ok, client} = KubeMQ.Client.start_link(
        address: "localhost:50000",
        client_id: "my-app",
        credential_provider: MyTokenProvider
      )
  """

  @doc """
  Retrieve an authentication token.

  Returns `{:ok, token}` on success or `{:error, reason}` on failure.
  The `opts` keyword list contains the client configuration for context.
  """
  @callback get_token(opts :: keyword()) :: {:ok, String.t()} | {:error, term()}
end
