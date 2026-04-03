defmodule KubeMQ do
  @moduledoc """
  KubeMQ Elixir SDK - Kubernetes-native messaging client.

  ## Quick Start

      {:ok, client} = KubeMQ.Client.start_link(address: "localhost:50000", client_id: "my-app")
      :ok = KubeMQ.Client.send_event(client, %KubeMQ.Event{channel: "test", body: "hello"})
      KubeMQ.Client.close(client)

  See `KubeMQ.Client` for the full API.
  """
end
