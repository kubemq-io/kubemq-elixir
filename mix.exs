defmodule KubeMQ.MixProject do
  use Mix.Project

  @version "0.1.0"
  @source_url "https://github.com/kubemq/kubemq-elixir"

  def project do
    [
      app: :kubemq,
      version: @version,
      elixir: "~> 1.15",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      docs: docs(),
      dialyzer: [plt_add_apps: [:ex_unit]],
      test_coverage: [
        tool: ExCoveralls,
        ignore_modules: [
          ~r/^Kubemq\./,
          ~r/^Google\./
        ]
      ],
      preferred_cli_env: [
        coveralls: :test,
        "coveralls.detail": :test,
        "coveralls.html": :test
      ],
      package: package(),
      description:
        "KubeMQ Elixir SDK — Kubernetes-native messaging client for events, commands, queries, and queues via gRPC",
      source_url: @source_url,
      aliases: aliases()
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  def application do
    [
      mod: {KubeMQ.Application, []},
      extra_applications: [:logger, :ssl]
    ]
  end

  defp deps do
    [
      {:grpc, "~> 0.11"},
      {:protobuf, "~> 0.14"},
      {:nimble_options, "~> 1.1"},
      {:telemetry, "~> 1.0"},
      {:jason, "~> 1.4"},
      {:broadway, "~> 1.0", optional: true},
      {:ex_doc, "~> 0.40", only: :dev, runtime: false},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:mox, "~> 1.2", only: :test},
      {:grpc_mock, "~> 0.3", only: :test},
      {:stream_data, "~> 1.0", only: :test},
      {:excoveralls, "~> 0.18", only: :test}
    ]
  end

  defp docs do
    [
      main: "KubeMQ",
      extras: [
        "README.md",
        "CHANGELOG.md",
        "CONCEPTS.md",
        "TROUBLESHOOTING.md",
        "COMPATIBILITY.md"
      ],
      groups_for_modules: [
        Core: [KubeMQ, KubeMQ.Client, KubeMQ.Error, KubeMQ.ServerInfo, KubeMQ.Subscription],
        Events: [KubeMQ.Event, KubeMQ.EventReceive, KubeMQ.EventStreamHandle],
        "Events Store": [
          KubeMQ.EventStore,
          KubeMQ.EventStoreResult,
          KubeMQ.EventStoreReceive,
          KubeMQ.EventStoreStreamHandle
        ],
        Commands: [
          KubeMQ.Command,
          KubeMQ.CommandReceive,
          KubeMQ.CommandResponse,
          KubeMQ.CommandReply
        ],
        Queries: [KubeMQ.Query, KubeMQ.QueryReceive, KubeMQ.QueryResponse, KubeMQ.QueryReply],
        Queues: [
          KubeMQ.QueueMessage,
          KubeMQ.QueuePolicy,
          KubeMQ.QueueAttributes,
          KubeMQ.QueueSendResult,
          KubeMQ.QueueBatchResult,
          KubeMQ.QueueReceiveResult,
          KubeMQ.QueueAckAllResult,
          KubeMQ.QueueUpstreamHandle,
          KubeMQ.PollRequest,
          KubeMQ.PollResponse
        ],
        "Channel Management": [KubeMQ.ChannelInfo],
        Broadway: [KubeMQ.Broadway.Events, KubeMQ.Broadway.EventsStore, KubeMQ.Broadway.Queues],
        Authentication: [KubeMQ.CredentialProvider, KubeMQ.StaticTokenProvider],
        Configuration: [KubeMQ.Config, KubeMQ.TLS, KubeMQ.RetryPolicy, KubeMQ.ReconnectPolicy]
      ]
    ]
  end

  defp package do
    [
      licenses: ["Apache-2.0"],
      links: %{
        "GitHub" => @source_url,
        "Documentation" => "https://hexdocs.pm/kubemq",
        "KubeMQ" => "https://kubemq.io"
      },
      files: ~w(lib .formatter.exs mix.exs README.md LICENSE CHANGELOG.md),
      keywords: [
        "kubemq",
        "messaging",
        "message-queue",
        "grpc",
        "kubernetes",
        "pub-sub",
        "queues",
        "elixir-sdk",
        "events",
        "rpc"
      ]
    ]
  end

  defp aliases do
    [
      lint: ["format --check-formatted", "credo --strict", "dialyzer"],
      "test.all": ["test --include integration"],
      "test.integration": ["test --only integration"]
    ]
  end
end
