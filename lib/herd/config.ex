defmodule Herd.Config do
  @moduledoc false

  @k 6
  @c 3
  @log_total_members round(:math.log10(10_000))
  @default_active_view_size @log_total_members + @c
  @default_passive_view_size @k * (@log_total_members + @c)
  @default_join_interval :timer.seconds(1)
  @default_neighbor_interval :timer.seconds(10)
  @default_shuffle_interval :timer.seconds(30)

  @spec arwl() :: non_neg_integer()
  def arwl,
    do: get_env(:arwl, 8)

  @spec prwl() :: non_neg_integer()
  def prwl,
    do: get_env(:prwl, 5)

  @spec shuffle_interval() :: non_neg_integer()
  def shuffle_interval,
    do: get_env(:shuffle_interval, @default_shuffle_interval)

  @spec join_interval() :: non_neg_integer()
  def join_interval,
    do: get_env(:join_interval, @default_join_interval)

  @spec join_timeout() :: non_neg_integer()
  def join_timeout,
    do: get_env(:join_timeout, 250)

  @spec contact_nodes() :: [Node.t()]
  def contact_nodes,
    do:
      :contact_nodes
      |> get_env([])
      |> Enum.filter(&Kernel.!=(&1, Node.self()))

  @spec active_view_size() :: non_neg_integer()
  def active_view_size,
    do: get_env(:active_view_size, @default_active_view_size)

  @spec passive_view_size() :: non_neg_integer()
  def passive_view_size,
    do: get_env(:passive_view_size, @default_passive_view_size)

  @spec neighbor_interval() :: non_neg_integer()
  def neighbor_interval,
    do: get_env(:neighbor_interval, @default_neighbor_interval)

  # private functions

  defp get_env(item, default),
    do: Application.get_env(:herd, item, default)
end
