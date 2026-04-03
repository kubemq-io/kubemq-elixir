defmodule KubeMQ.ChannelInfo do
  @moduledoc """
  Information about a KubeMQ channel returned from list operations.

  ## Fields

    * `name` - Channel name
    * `type` - Channel type string (e.g., "events", "queues")
    * `last_activity` - Unix timestamp of last activity
    * `is_active` - Whether the channel currently has active clients
    * `incoming` - Incoming message statistics
    * `outgoing` - Outgoing message statistics

  ## Usage

      iex> info = %KubeMQ.ChannelInfo{name: "orders", type: "queues"}
      iex> info.name
      "orders"
      iex> info.is_active
      false
  """

  @type t :: %__MODULE__{
          name: String.t(),
          type: String.t(),
          last_activity: integer(),
          is_active: boolean(),
          incoming: non_neg_integer(),
          outgoing: non_neg_integer()
        }

  defstruct name: "",
            type: "",
            last_activity: 0,
            is_active: false,
            incoming: 0,
            outgoing: 0

  @doc """
  Parse a list of channel info structs from a JSON-encoded response body.
  """
  @spec from_json(binary()) :: {:ok, [t()]} | {:error, term()}
  def from_json(data) when is_binary(data) and byte_size(data) == 0 do
    {:ok, []}
  end

  def from_json(data) when is_binary(data) do
    case Jason.decode(data) do
      {:ok, items} when is_list(items) ->
        {:ok, Enum.map(items, &from_map/1)}

      {:ok, _} ->
        {:ok, []}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @spec from_map(map()) :: t()
  defp from_map(item) when is_map(item) do
    %__MODULE__{
      name: Map.get(item, "name", ""),
      type: Map.get(item, "type", ""),
      last_activity: Map.get(item, "lastActivity", 0),
      is_active: Map.get(item, "isActive", false),
      incoming: extract_messages(Map.get(item, "incoming")),
      outgoing: extract_messages(Map.get(item, "outgoing"))
    }
  end

  defp extract_messages(nil), do: 0
  defp extract_messages(stats) when is_map(stats), do: Map.get(stats, "messages", 0)
  defp extract_messages(n) when is_integer(n), do: n
  defp extract_messages(_), do: 0
end
