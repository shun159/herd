defmodule Herd.Plumtree.Manager do
  @moduledoc false

  use GenServer

  import Logger

  alias __MODULE__, as: State
  alias Herd.Hyparview.Membership

  defstruct eager_push_peers: MapSet.new(),
            lazy_push_peers: MapSet.new(),
            lazy_queues: MapSet.new(),
            missing: MapSet.new(),
            received_msgs: MapSet.new()

  @typep t :: %State{
           eager_push_peers: MapSet.t(Node.t()),
           lazy_push_peers: MapSet.t(Node.t()),
           lazy_queues: MapSet.t(),
           missing: MapSet.t(),
           received_msgs: MapSet.t()
         }

  # API functions

  def start_link do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  # GenServer callback functions

  @impl GenServer
  def init(_) do
    :ok = info("Plumtree Tree Manager started on #{Node.self()}")
    :ok = Membership.subscribe()
    {:ok, %State{}}
  end

  @impl GenServer
  def handle_call(_request, _from, state) do
    {:reply, :ok, state}
  end

  @impl GenServer
  def handle_cast(_request, state) do
    {:noreply, state}
  end

  @impl GenServer
  def handle_info({:membership, node, :up}, state) do
    :ok = info("membership event: node: #{node} UP")
    eager_push_peers = MapSet.put(state.eager_push_peers, node)
    {:noreply, %{state | eager_push_peers: eager_push_peers}}
  end

  @impl GenServer
  def handle_info({:membership, node, :down}, state) do
    :ok = warn("membership event: node: #{node} DOWN")
    eager_push_peers = MapSet.delete(state.eager_push_peers, node)
    lazy_push_peers = MapSet.delete(state.lazy_push_peers, node)
    {:noreply, %{state | eager_push_peers: eager_push_peers, lazy_push_peers: lazy_push_peers}}
  end

  @impl GenServer
  def handle_info(_info, state) do
    {:noreply, state}
  end

  @impl GenServer
  def terminate(_reason, state) do
    :ok = Membership.unsubscribe()
  end
end
