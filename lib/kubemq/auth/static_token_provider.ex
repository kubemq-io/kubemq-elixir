defmodule KubeMQ.StaticTokenProvider do
  @moduledoc """
  Static token credential provider for KubeMQ authentication.

  Returns a fixed token string. Useful for development or when tokens are
  managed externally and injected via environment variables.

  ## Usage

      {:ok, client} = KubeMQ.Client.start_link(
        address: "localhost:50000",
        client_id: "my-app",
        credential_provider: KubeMQ.StaticTokenProvider,
        auth_token: "my-jwt-token"
      )
  """

  @behaviour KubeMQ.CredentialProvider

  @doc """
  Returns the static token from the `:auth_token` key in opts.
  """
  @impl true
  @spec get_token(keyword()) :: {:ok, String.t()} | {:error, term()}
  def get_token(opts) do
    case Keyword.get(opts, :auth_token) do
      nil -> {:error, :no_token_configured}
      "" -> {:error, :empty_token}
      token when is_binary(token) -> {:ok, token}
      _ -> {:error, :invalid_token_type}
    end
  end
end
