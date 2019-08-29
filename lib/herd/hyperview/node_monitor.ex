defmodule Herd.Hyparview.NodeMonitor do
  @moduledoc false

  use GenServer

  import Logger

  alias __MODULE__, as: State

  defstruct connected_nodes: MapSet.new(),
            subscribers: MapSet.new()

  @typep t :: %State{
           connected_nodes: MapSet.t(Node.t()),
           subscribers: MapSet.t(pid())
         }

  @request_timeout :timer.seconds(3)

  # API functions

  @spec subscribe() :: :ok
  def subscribe,
    do: GenServer.cast(__MODULE__, {:subscribe, self()})

  @spec unsubscribe() :: :ok
  def unsubscribe,
    do: GenServer.cast(__MODULE__, {:unsubscribe, self()})

  @spec is_connected?(Node.t()) :: boolean()
  def is_connected?(node),
    do: GenServer.call(__MODULE__, {:is_connected?, node})

  @spec connect(Node.t()) :: :ok | {:error, :timeout}
  def connect(node),
    do: GenServer.call(__MODULE__, {:connect, node})

  @spec disconnect(Node.t()) :: :ok | {:error, :timeout}
  def disconnect(node),
    do: GenServer.call(__MODULE__, {:disconnect, node})

  @spec disconnect(Node.t(), pos_integer()) :: :ok
  def disconnect(node, after_msec),
    do: GenServer.cast(__MODULE__, {:disconnect, node, after_msec})

  @spec start_link() :: GenServer.on_start()
  def start_link,
    do: GenServer.start_link(__MODULE__, [], name: __MODULE__)

  # GenServer callback functions

  @impl GenServer
  def init(_args) do
    :ok = info("NodeMonitor started on #{Node.self()}")
    {:ok, %State{}}
  end

  @impl GenServer
  def handle_continue({:connect, node, from}, state) do
    tref = Process.send_after(__MODULE__, {:timeout, node, from}, @request_timeout)
    :ok = connect_request(node, from, tref)
    {:noreply, state}
  end

  @impl GenServer
  def handle_continue({:disconnect, node}, state) do
    _ = Node.disconnect(node)
    :ok = :aten.unregister(node)
    new_nodes = MapSet.delete(state.connected_nodes, node)
    {:noreply, %{state | connected_nodes: new_nodes}}
  end

  @impl GenServer
  def handle_call({:is_connected?, node}, _from, state),
    do: {:reply, Enum.member?(state.connected_nodes, node), state}

  @impl GenServer
  def handle_call({:connect, node}, from, state) do
    if Enum.member?(state.connected_nodes, node) do
      {:reply, :ok, state}
    else
      {:noreply, state, {:continue, {:connect, node, from}}}
    end
  end

  @impl GenServer
  def handle_call({:disconnect, node}, _from, state) do
    if Enum.member?(state.connected_nodes, node) do
      {:reply, :ok, state, {:continue, {:disconnect, node}}}
    else
      {:reply, :ok, state}
    end
  end

  @impl GenServer
  def handle_cast(%{msg_type: :connect_request} = request, state) do
    :ok = info("Connect Request from #{request.sender}")
    :ok = connect_reply(request.sender, request)
    :ok = notify(%{type: :connected, data: %{node: request.sender}}, state)
    state = register_node(request.sender, state)
    {:noreply, state}
  end

  @impl GenServer
  def handle_cast(%{msg_type: :connect_reply, request: r} = reply, state) do
    :ok = info("Connect Reply from #{reply.sender}")
    :ok = cancel_timer(r.timer)
    :ok = GenServer.reply(r.client, :ok)
    state = register_node(reply.sender, state)
    {:noreply, state}
  end

  @impl GenServer
  def handle_cast({:disconnect, node, after_msec}, state) do
    if Enum.member?(state.connected_nodes, node) do
      _tref = Process.send_after(__MODULE__, {:disconnect, node}, after_msec)
      {:noreply, state}
    else
      {:noreply, state}
    end
  end

  @impl GenServer
  def handle_cast({:subscribe, pid}, state) do
    :ok = debug("Subscribe node_event from #{inspect(pid)}")
    {:noreply, %{state | subscribers: MapSet.put(state.subscribers, pid)}}
  end

  @impl GenServer
  def handle_cast({:unsubscribe, pid}, state) do
    :ok = debug("Unsubscribe node_event from #{inspect(pid)}")
    {:noreply, %{state | subscribers: MapSet.delete(state.subscribers, pid)}}
  end

  @impl GenServer
  def handle_info({:disconnect, node}, state) do
    {:noreply, state, {:continue, {:disconnect, node}}}
  end

  @impl GenServer
  def handle_info({:timeout, node, client}, state) do
    :ok = warn("Failed to connect: node = #{node}")
    # Ensure disconnected
    _ = Node.disconnect(node)
    :ok = GenServer.reply(client, {:error, :timeout})
    {:noreply, state}
  end

  @impl GenServer
  def handle_info({:node_event, node, :down}, state) do
    :ok = warn("Disconnected: node = #{node}")
    :ok = notify(%{type: :disconnected, data: %{node: node}}, state)
    {:noreply, state, {:continue, {:disconnect, node}}}
  end

  @impl GenServer
  def handle_info({:node_event, node, :up}, state) do
    new_nodes = MapSet.put(state.connected_nodes, node)
    {:noreply, %{state | connected_nodes: new_nodes}}
  end

  # private functions

  @spec connect_request(Node.t(), GenServer.from(), reference()) :: :ok
  defp connect_request(node, from, tref),
    do:
      send_message(
        node,
        %{
          msg_type: :connect_request,
          sender: Node.self(),
          timer: tref,
          client: from
        }
      )

  @spec connect_reply(Node.t(), request :: map()) :: :ok
  defp connect_reply(node, request),
    do:
      send_message(
        node,
        %{
          msg_type: :connect_reply,
          sender: Node.self(),
          request: request
        }
      )

  @spec send_message(Node.t(), msg :: term()) :: :ok
  defp send_message(node, msg),
    do: GenServer.cast({__MODULE__, node}, msg)

  @spec cancel_timer(reference() | any()) :: :ok
  defp cancel_timer(tref) do
    _ = if is_reference(tref), do: Process.cancel_timer(tref)
    :ok
  end

  @spec register_node(Node.t(), t()) :: t()
  defp register_node(node, state) do
    :ok = :aten.register(node)
    new_nodes = MapSet.put(state.connected_nodes, node)
    %{state | connected_nodes: new_nodes}
  end

  @spec notify(map(), t()) :: :ok
  defp notify(msg, state) do
    Enum.each(
      state.subscribers,
      &Process.send(&1, msg, [])
    )
  end
end
