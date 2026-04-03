defmodule KubeMQ.CommandResponse do
  @moduledoc """
  Response received after sending a command via `KubeMQ.Client.send_command/2`.

  ## Fields

    * `command_id` (`String.t()`) — ID of the command that was executed.
    * `executed` (`boolean()`) — Whether the command was executed successfully.
    * `executed_at` (`integer()`) — Unix timestamp when the command was executed.
    * `error` (`String.t() | nil`) — Error message if execution failed.
    * `tags` (`%{String.t() => String.t()}`) — Key-value tags from the responder.
  """

  @type t :: %__MODULE__{
          command_id: String.t(),
          executed: boolean(),
          executed_at: integer(),
          error: String.t() | nil,
          tags: %{String.t() => String.t()}
        }

  defstruct [:command_id, :executed, :executed_at, :error, tags: %{}]

  @spec new(keyword()) :: t()
  def new(opts \\ []) do
    %__MODULE__{
      command_id: Keyword.get(opts, :command_id, ""),
      executed: Keyword.get(opts, :executed, false),
      executed_at: Keyword.get(opts, :executed_at, 0),
      error: Keyword.get(opts, :error),
      tags: Keyword.get(opts, :tags, %{})
    }
  end

  @spec from_response(map()) :: t()
  def from_response(response) do
    %__MODULE__{
      command_id: response.request_id || "",
      executed: response.executed || false,
      executed_at: response.timestamp || 0,
      error: if(response.error in [nil, ""], do: nil, else: response.error),
      tags: response.tags || %{}
    }
  end
end
