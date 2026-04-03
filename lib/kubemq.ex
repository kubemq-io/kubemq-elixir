defmodule KubeMQ do
  @moduledoc """
  KubeMQ Elixir SDK — Kubernetes-native messaging client.

  Provides a full-featured Elixir client for [KubeMQ](https://kubemq.io), supporting
  four messaging patterns over gRPC: events (pub/sub), events store (persistent pub/sub),
  commands/queries (RPC), and queues (pull-based messaging).

  ## Quick Start

      {:ok, client} = KubeMQ.Client.start_link(address: "localhost:50000", client_id: "my-app")
      :ok = KubeMQ.Client.send_event(client, %KubeMQ.Event{channel: "test", body: "hello"})
      KubeMQ.Client.close(client)

  ## Core Modules

    * `KubeMQ.Client` — Primary API surface for all messaging operations
    * `KubeMQ.Config` — NimbleOptions-based configuration schema
    * `KubeMQ.Error` — Typed error struct with error codes and retryability

  ## Messaging Patterns

    * **Events** — `KubeMQ.Event`, `KubeMQ.EventReceive`, `KubeMQ.EventStreamHandle`
    * **Events Store** — `KubeMQ.EventStore`, `KubeMQ.EventStoreReceive`, `KubeMQ.EventStoreStreamHandle`
    * **Commands** — `KubeMQ.Command`, `KubeMQ.CommandReceive`, `KubeMQ.CommandResponse`, `KubeMQ.CommandReply`
    * **Queries** — `KubeMQ.Query`, `KubeMQ.QueryReceive`, `KubeMQ.QueryResponse`, `KubeMQ.QueryReply`
    * **Queues** — `KubeMQ.QueueMessage`, `KubeMQ.PollResponse`, `KubeMQ.QueueUpstreamHandle`

  ## Integration

    * `KubeMQ.Broadway.Events` / `KubeMQ.Broadway.EventsStore` / `KubeMQ.Broadway.Queues` — Broadway producers
    * `KubeMQ.TLS` — TLS/mTLS configuration
    * `KubeMQ.CredentialProvider` — Dynamic authentication
  """
end
