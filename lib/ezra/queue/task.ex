defmodule Ezra.Queue.Task do
  @moduledoc """
  Struct representing a single task row, with helpers for constructing from
  a raw SQLite row and validating status transitions.

  Status machine:
    available → in_flight → done
    in_flight → available  (timeout or nack, attempts < max)
    in_flight → dead       (nack or timeout, attempts >= max)
  """

  @statuses ~w(available in_flight done dead)

  @enforce_keys [:id, :queue, :payload, :status, :attempts, :max_attempts,
                 :inserted_at, :scheduled_at, :visibility_timeout]

  defstruct [
    :id,
    :queue,
    :payload,
    :status,
    :attempts,
    :max_attempts,
    :inserted_at,
    :scheduled_at,
    :claimed_at,
    :worker_id,
    :visibility_timeout,
    :expires_at,
    :last_error
  ]

  @type status :: :available | :in_flight | :done | :dead

  @type t :: %__MODULE__{
          id: pos_integer(),
          queue: String.t(),
          payload: binary(),
          status: status(),
          attempts: non_neg_integer(),
          max_attempts: pos_integer(),
          inserted_at: pos_integer(),
          scheduled_at: pos_integer(),
          claimed_at: pos_integer() | nil,
          worker_id: String.t() | nil,
          visibility_timeout: pos_integer(),
          expires_at: pos_integer() | nil,
          last_error: String.t() | nil
        }

  @columns ~w(id queue payload status attempts max_attempts inserted_at scheduled_at
              claimed_at worker_id visibility_timeout expires_at last_error)a

  @doc """
  Builds a Task struct from a raw SQLite row (list of values in column order).
  Column order must match `@columns`.
  """
  @spec from_row([term()]) :: t()
  def from_row(row) when length(row) == length(@columns) do
    @columns
    |> Enum.zip(row)
    |> Map.new()
    |> then(&struct!(__MODULE__, &1))
  end

  @doc "Returns the list of column names used in SELECT queries."
  @spec select_columns() :: String.t()
  def select_columns do
    Enum.join(@columns, ", ")
  end

  @doc "Returns true if transitioning from `from` to `to` is valid."
  @spec valid_transition?(atom(), atom()) :: boolean()
  def valid_transition?(:available, :in_flight), do: true
  def valid_transition?(:in_flight, :done), do: true
  def valid_transition?(:in_flight, :available), do: true
  def valid_transition?(:in_flight, :dead), do: true
  def valid_transition?(_from, _to), do: false

  @doc "All valid status strings."
  @spec statuses() :: [String.t()]
  def statuses, do: @statuses
end
