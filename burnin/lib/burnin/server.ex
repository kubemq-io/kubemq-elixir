defmodule Burnin.Server do
  @moduledoc """
  HTTP control plane using Plug.Router + Bandit.
  Exposes 12 endpoints matching the Go burn-in HTTP API.
  """

  use Plug.Router

  plug(:match)
  plug(Plug.Parsers, parsers: [:json], json_decoder: Jason)
  plug(:dispatch)

  get "/health" do
    send_json(conn, 200, %{status: "alive"})
  end

  get "/ready" do
    status = Burnin.Engine.status()
    state = status.state

    case state do
      s when s in ["starting", "stopping"] ->
        send_json(conn, 503, %{status: "not_ready", state: state})

      _ ->
        send_json(conn, 200, %{status: "ready", state: state})
    end
  end

  get "/info" do
    info = Burnin.Engine.info()
    send_json(conn, 200, info)
  end

  get "/broker/status" do
    result = Burnin.Engine.broker_status()
    send_json(conn, 200, result)
  end

  post "/run/start" do
    body = read_body_text(conn)

    case Burnin.Engine.start_run(body) do
      {:ok, run_id} ->
        send_json(conn, 202, %{run_id: run_id, state: "starting", message: "Run started"})

      {:error, reason} ->
        send_json(conn, 400, %{error: reason})
    end
  end

  post "/run/stop" do
    case Burnin.Engine.stop_run() do
      :ok ->
        status = Burnin.Engine.run_status()
        send_json(conn, 202, %{run_id: status[:run_id], state: status[:state], message: "Run stopping"})

      {:error, reason} ->
        send_json(conn, 400, %{error: reason})
    end
  end

  get "/run" do
    status = Burnin.Engine.run_status()
    send_json(conn, 200, status)
  end

  get "/run/status" do
    status = Burnin.Engine.run_status()
    send_json(conn, 200, status)
  end

  get "/run/config" do
    config = Burnin.Engine.run_config()

    case config do
      nil -> send_json(conn, 200, %{config: nil})
      cfg -> send_json(conn, 200, Map.from_struct(cfg))
    end
  end

  get "/run/report" do
    report = Burnin.Engine.run_report()
    send_json(conn, 200, report)
  end

  post "/cleanup" do
    case Burnin.Engine.cleanup() do
      :ok -> send_json(conn, 200, %{status: "cleaned up"})
      {:error, reason} -> send_json(conn, 500, %{error: reason})
    end
  end

  get "/metrics" do
    text = Burnin.Metrics.scrape()
    send_text(conn, 200, text)
  end

  match _ do
    send_json(conn, 404, %{error: "not found"})
  end

  defp send_json(conn, status, body) do
    conn
    |> set_cors()
    |> put_resp_content_type("application/json")
    |> send_resp(status, Jason.encode!(body))
  end

  defp send_text(conn, status, text) do
    conn
    |> set_cors()
    |> put_resp_content_type("text/plain")
    |> send_resp(status, text)
  end

  defp set_cors(conn) do
    conn
    |> put_resp_header("access-control-allow-origin", "*")
    |> put_resp_header("access-control-allow-methods", "GET, POST, OPTIONS")
    |> put_resp_header("access-control-allow-headers", "Content-Type")
  end

  defp read_body_text(conn) do
    case conn.body_params do
      %Plug.Conn.Unfetched{} ->
        {:ok, body, _conn} = Plug.Conn.read_body(conn)
        body

      %{"_json" => _} = params ->
        Jason.encode!(params)

      params when is_map(params) ->
        Jason.encode!(params)
    end
  end
end
