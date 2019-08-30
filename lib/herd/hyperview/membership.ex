defmodule Herd.Hyparview.Membership do
  @moduledoc false

  use GenServer

  import Logger

  alias __MODULE__, as: State
  alias Herd.Config
  alias Herd.Hyparview.NodeMonitor

  defstruct active_view: MapSet.new(),
            passive_view: MapSet.new(),
            arwl: 0,
            prwl: 0,
            shuffle_interval: 0,
            join_interval: 0,
            join_timeout: 0,
            neighbor_interval: 0,
            active_view_size: 0,
            passive_view_size: 0,
            joined?: false,
            subscriber: MapSet.new()

  @typep t :: %State{
           active_view: MapSet.t(Node.t()),
           passive_view: MapSet.t(Node.t()),
           arwl: non_neg_integer(),
           prwl: non_neg_integer(),
           shuffle_interval: non_neg_integer(),
           join_interval: non_neg_integer(),
           join_timeout: non_neg_integer(),
           neighbor_interval: non_neg_integer(),
           active_view_size: non_neg_integer(),
           passive_view_size: non_neg_integer(),
           joined?: boolean(),
           subscriber: MapSet.t(pid())
         }

  @typep join :: %{
           msg: :join,
           sender: Node.t(),
           tref: reference()
         }

  @typep join_ack :: %{
           msg: :join_ack,
           sender: Node.t(),
           tref: reference(),
           active: MapSet.t(Node.t())
         }

  @typep forward_join :: %{
           msg: :forward_join,
           sender: Node.t(),
           new_node: Node.t(),
           ttl: non_neg_integer(),
           prwl: non_neg_integer(),
           adv_path: MapSet.t(Node.t())
         }

  @typep neighbor :: %{
           msg: :neighbor,
           sender: Node.t(),
           priority: :low | :high
         }

  @typep neighbor_ack :: %{
           msg: :neighbor_ack,
           sender: Node.t()
         }

  @typep neighbor_nak :: %{
           msg: :neighbor_nak,
           sender: Node.t(),
           passive_view: MapSet.t(Node.t())
         }

  @typep shuffle :: %{
           msg: :shuffle,
           sender: Node.t(),
           origin: Node.t(),
           ttl: non_neg_integer(),
           passive_view: MapSet.t(Node.t()),
           active_view: MapSet.t(Node.t()),
           adv_path: MapSet.t(Node.t())
         }

  @typep shuffle_reply :: %{
           msg: :shuffle_reply,
           sender: Node.t(),
           combined_view: MapSet.t(Node.t())
         }

  @typep disconnect :: %{
           msg: :disconnect,
           sender: Node.t()
         }

  # API functions

  @spec get_active_view() :: {:ok, MapSet.t(Node.t())} | {:error, reason :: term()}
  def get_active_view, do: GenServer.call(__MODULE__, :get_active_view)

  @spec get_passive_view() :: {:ok, MapSet.t(Node.t())} | {:error, reason :: term()}
  def get_passive_view, do: GenServer.call(__MODULE__, :get_passive_view)

  @spec subscribe() :: :ok
  def subscribe, do: GenServer.cast(__MODULE__, {:subscribe, self()})

  @spec unsubscribe() :: :ok
  def unsubscribe, do: GenServer.cast(__MODULE__, {:unsubscribe, self()})

  @spec start_link() :: GenServer.on_start()
  def start_link,
    do: GenServer.start_link(__MODULE__, [], name: __MODULE__)

  # GenServer callback functions

  @impl GenServer
  def init(_args) do
    :ok = info("Membership started on #{Node.self()}")
    :ok = NodeMonitor.subscribe()
    seed = :erlang.phash2({Node.self(), self(), :erlang.monotonic_time()})
    :rand.seed(:exsplus, seed)

    {:ok,
     %State{
       passive_view: MapSet.new(Config.contact_nodes()),
       arwl: Config.arwl(),
       prwl: Config.prwl(),
       shuffle_interval: Config.shuffle_interval(),
       join_interval: Config.join_interval(),
       join_timeout: Config.join_timeout(),
       neighbor_interval: Config.neighbor_interval(),
       active_view_size: Config.active_view_size(),
       passive_view_size: Config.passive_view_size()
     }, {:continue, :init}}
  end

  @impl GenServer
  def handle_continue(:init, state) do
    _ = Process.send_after(self(), :sched_join, state.join_interval)
    _ = Process.send_after(self(), :sched_shuf, state.shuffle_interval)
    {:noreply, state}
  end

  @impl GenServer
  def handle_continue(:join, state) do
    if join(state) == :ok,
      do: :ok,
      else: Process.send_after(self(), :sched_join, state.join_interval)

    {:noreply, state}
  end

  @impl GenServer
  def handle_continue(:shuffle, state) do
    _ = shuffle(state)
    {:noreply, state}
  end

  @impl GenServer
  def handle_continue({:disconn, node}, state) do
    _ = maybe_disconnect(node, state)
    {:noreply, state}
  end

  @impl GenServer
  def handle_continue(:neighbor, state) do
    case neighbor(state) do
      {:ok, state} ->
        {:noreply, state}

      {:error, :nonode} ->
        :ok = warn("Passive view is empty, couldn't attempt NEIGHBOR")
        {:noreply, state}

      {:error, _reason} ->
        {:noreply, state}
    end
  end

  @impl GenServer
  def handle_call(:get_active_view, _from, state) do
    {:reply, {:ok, state.active_view}, state}
  end

  @impl GenServer
  def handle_call(:get_passive_view, _from, state) do
    {:reply, {:ok, state.passive_view}, state}
  end

  @impl GenServer
  def handle_cast({:subscribe, pid}, state) do
    {:noreply, %{state | subscriber: MapSet.put(state.subscriber, pid)}}
  end

  @impl GenServer
  def handle_cast({:unsubscribe, pid}, state) do
    {:noreply, %{state | subscriber: MapSet.delete(state.subscriber, pid)}}
  end

  ## HyParView message handlers

  @impl GenServer
  def handle_cast(%{msg: :join, sender: sender} = join, state) do
    :ok = debug("Received JOIN message from #{sender}")
    {_, state1} = handle_join(join, state)
    {:noreply, state1}
  end

  @impl GenServer
  def handle_cast(%{msg: :join_ack, sender: sender} = join_ack, state) do
    :ok = debug("Received JOIN_ACK message from #{sender}")
    {_, state1} = handle_join_ack(join_ack, state)
    {:noreply, state1}
  end

  @impl GenServer
  def handle_cast(%{msg: :forward_join, sender: sender, new_node: node} = forward_join, state) do
    :ok =
      debug(
        "Received FORWARD_JOIN message from #{sender} ttl = #{forward_join.ttl} new = #{node}"
      )

    {_, state1} = handle_forward_join(forward_join, state)
    {:noreply, state1}
  end

  @impl GenServer
  def handle_cast(%{msg: :neighbor, sender: sender} = neighbor, state) do
    :ok = debug("Received NEIGHBOR message from #{sender}")
    {_, state1} = handle_neighbor(neighbor, state)
    {:noreply, state1}
  end

  @impl GenServer
  def handle_cast(%{msg: :neighbor_ack, sender: sender} = neighbor_ack, state) do
    :ok = debug("Received NEIGHBOR_ACK message from #{sender}")
    {_, state1} = handle_neighbor_ack(neighbor_ack, state)
    {:noreply, state1}
  end

  @impl GenServer
  def handle_cast(%{msg: :neighbor_nak, sender: sender} = neighbor_nak, state) do
    :ok = debug("Received NEIGHBOR_NAK message from #{sender}")
    {_, state1} = handle_neighbor_nak(neighbor_nak, state)
    {:noreply, state1}
  end

  @impl GenServer
  def handle_cast(%{msg: :shuffle, sender: sender, origin: origin, ttl: ttl} = shuffle, state) do
    :ok = debug("Received SHUFFLE message from #{sender} origin = #{origin} ttl = #{ttl}")
    {_, state1} = handle_shuffle(shuffle, state)
    {:noreply, state1}
  end

  @impl GenServer
  def handle_cast(%{msg: :shuffle_reply, sender: sender} = shuffle_reply, state) do
    :ok = debug("Received SHUFFLE_REPLY message from #{sender}")
    {_, state1} = handle_shuffle_reply(shuffle_reply, state)
    {:noreply, state1}
  end

  @impl GenServer
  def handle_cast(%{msg: :disconnect, sender: sender} = disconnect, state) do
    :ok = debug("Received DISCONNECT message from #{sender}")
    {_, state1} = handle_disconnect(disconnect, state)
    {:noreply, state1}
  end

  ## Scheduled HyParView protocol operations

  @impl GenServer
  def handle_info(:sched_join, %State{joined?: joined?} = state) do
    if joined?, do: {:noreply, state}, else: {:noreply, state, {:continue, :join}}
  end

  @impl GenServer
  def handle_info(:sched_shuf, state) do
    _ = Process.send_after(self(), :sched_shuf, shuffle_interval(state))
    {:noreply, state, {:continue, :shuffle}}
  end

  @impl GenServer
  def handle_info({:sched_disconn, node}, state) do
    {:noreply, state, {:continue, {:disconn, node}}}
  end

  @impl GenServer
  def handle_info(:sched_neighbor, state) do
    {:noreply, state, {:continue, :neighbor}}
  end

  ## NODE MONITOR events

  @impl GenServer
  def handle_info(%{type: :connected}, state) do
    # TODO:
    {:noreply, state}
  end

  @impl GenServer
  def handle_info(%{type: :disconnected, data: %{node: node}}, state) do
    if MapSet.member?(state.active_view, node) do
      :ok = warn("Node in active_view member has failed, attempting neighbor request")
      active = MapSet.delete(state.active_view, node)
      passive = MapSet.delete(state.active_view, node)
      :ok = notify_event(node, :down, state)
      {:noreply, %{state | active_view: active, passive_view: passive}, {:continue, :neighbor}}
    else
      {:noreply, state}
    end
  end

  @impl GenServer
  def handle_info({:join_timeout, node}, state) do
    :ok = info("JOIN timeout: node=#{node}")
    _ = Process.send_after(self(), :sched_join, state.join_interval)
    {:noreply, state}
  end

  # private functions

  @spec handle_join(join(), t()) :: {:ok, t()} | {:error, t()}
  defp handle_join(%{sender: sender} = join, state) do
    case NodeMonitor.connect(sender) do
      {:error, _reason} ->
        :ok = warn("Received JOIN from #{sender}, but couldn't connect back to it")
        {:error, state}

      _ when sender == node() ->
        {:error, state}

      :ok ->
        state = try_add_node_to_active_view(sender, state)
        :ok = notify_event(sender, :up, state)
        :ok = send_join_ack_message(sender, join.tref, state)
        :ok = bcast_forward_join(state, sender)
        {:ok, state}
    end
  end

  @spec handle_join_ack(join_ack(), t()) :: {:ok, t()} | {:error, t()}
  defp handle_join_ack(%{sender: sender} = join_ack, state) do
    :ok = cancel_timer(join_ack.tref)
    state1 = try_add_nodes_to_passive_view(join_ack.passive_view, state)
    state2 = try_add_node_to_active_view(sender, state1)
    :ok = notify_event(sender, :up, state)
    {:ok, %{state2 | joined?: true}}
  end

  @spec handle_forward_join(forward_join(), t()) :: {:ok, t()}
  defp handle_forward_join(%{new_node: node} = forward_join, state) do
    state1 = handle_forward_join_1(forward_join, state)

    if not MapSet.member?(state1.active_view, node),
      do: forward_forward_join(forward_join, state1)

    {:ok, state1}
  end

  @spec handle_neighbor(neighbor(), t()) :: {:ok, t()}
  defp handle_neighbor(%{priority: :high} = neighbor, state) do
    state1 = try_add_node_to_active_view(neighbor.sender, state)
    :ok = send_neighbor_ack(neighbor.sender)
    {:ok, state1}
  end

  @spec handle_neighbor(neighbor(), t()) :: {:ok, t()}
  defp handle_neighbor(%{priority: :low} = neighbor, state) do
    if MapSet.size(state.active_view) >= state.active_view_size do
      :ok = send_neighbor_nak(neighbor.sender, state.passive_view)
      _ = maybe_disconnect(neighbor.sender, state)
      {:ok, state}
    else
      state1 = try_add_node_to_active_view(neighbor.sender, state)
      :ok = notify_event(neighbor.sender, :up, state)
      :ok = send_neighbor_ack(neighbor.sender)
      {:ok, state1}
    end
  end

  @spec handle_neighbor_ack(neighbor_ack(), t()) :: {:ok, t()}
  defp handle_neighbor_ack(neighbor_ack, state) do
    state1 = try_add_node_to_active_view(neighbor_ack.sender, state)
    :ok = notify_event(neighbor_ack.sender, :up, state)
    {:ok, state1}
  end

  @spec handle_neighbor_nak(neighbor_nak(), t()) :: {:ok, t()}
  defp handle_neighbor_nak(neighbor_nak, state) do
    passive_view =
      neighbor_nak.passive_view
      |> MapSet.union(state.passive_view)
      |> MapSet.difference(state.active_view)
      |> MapSet.delete(Node.self())
      |> try_add_nodes_to_passive_view(state)

    {:ok, %{state | passive_view: passive_view}}
  end

  @spec handle_shuffle(shuffle(), t()) :: {:ok, t()}
  defp handle_shuffle(%{ttl: 0} = shuffle, state) do
    combined_view = combine_with_remote_view(shuffle, state)
    :ok = send_shuffle_reply(shuffle.origin, combined_view)
    _ = maybe_disconnect(shuffle.sender, state)
    state1 = merge_passive_view(shuffle, state)
    {:ok, state1}
  end

  defp handle_shuffle(%{} = shuffle, state) do
    target =
      state.active_view
      |> MapSet.delete(shuffle.origin)
      |> MapSet.delete(shuffle.sender)
      |> MapSet.difference(shuffle.adv_path)
      |> select_node()

    case target do
      {:ok, node} ->
        send_shuffle(
          node,
          shuffle.origin,
          shuffle.ttl - 1,
          shuffle.passive_view,
          shuffle.active_view,
          MapSet.put(shuffle.adv_path, Node.self())
        )

        {:ok, state}

      {:error, _} ->
        handle_shuffle(%{shuffle | ttl: 0}, state)
    end
  end

  @spec handle_shuffle_reply(shuffle_reply(), t()) :: {:ok, t()}
  defp handle_shuffle_reply(shuffle_reply, state) do
    state1 =
      shuffle_reply.combined_view
      |> MapSet.union(state.passive_view)
      |> MapSet.difference(state.active_view)
      |> MapSet.delete(Node.self())
      |> try_add_nodes_to_passive_view(state)

    {:ok, state1}
  end

  @spec handle_disconnect(disconnect(), t()) :: {:ok, t()}
  defp handle_disconnect(disconnect, state) do
    active_view = MapSet.delete(state.active_view, disconnect.sender)
    passive_view = MapSet.put(state.passive_view, disconnect.sender)
    state1 = %{state | active_view: active_view, passive_view: passive_view}
    :ok = notify_event(disconnect.sender, :down, state)
    _ = maybe_disconnect(disconnect.sender, state1)
    {:ok, state1}
  end

  @spec handle_forward_join_1(forward_join(), t()) :: {:ok, t()}
  defp handle_forward_join_1(%{ttl: 0, new_node: node}, state) do
    :ok = notify_event(node, :up, state)
    try_add_node_to_active_view(node, state)
  end

  defp handle_forward_join_1(%{ttl: prwl, new_node: node}, %{prwl: prwl} = state),
    do: try_add_node_to_passive_view(node, state)

  defp handle_forward_join_1(%{new_node: node}, state) do
    case MapSet.size(state.active_view) do
      1 ->
        :ok = notify_event(node, :up, state)
        state1 = try_add_node_to_active_view(node, state)
        priority = neighbor_priority(state)
        :ok = send_neighbor(node, priority)
        state1

      _ ->
        state
    end
  end

  @spec forward_forward_join(forward_join(), t()) :: :ok
  defp forward_forward_join(%{new_node: node, ttl: ttl, adv_path: path}, state) do
    targets =
      state.active_view
      |> Enum.filter(&(not MapSet.member?(path, &1)))
      |> Enum.take_random(1)

    case targets do
      [target] ->
        send_forward_join_message(
          target,
          node,
          ttl - 1,
          MapSet.put(path, Node.self()),
          state
        )

      _ ->
        :ok
    end
  end

  @spec try_add_node_to_active_view(Node.t(), t()) :: t()
  defp try_add_node_to_active_view(node, %State{active_view_size: size} = state) do
    case MapSet.size(state.active_view) do
      active_size when active_size >= size ->
        state1 =
          state.active_view
          # drop random elements from active view
          |> Enum.take_random(active_size - (size - 1))
          |> Enum.reduce(state, &disconnect(&1, &2))

        add_node_to_active_view(node, state1)

      _ ->
        add_node_to_active_view(node, state)
    end
  end

  @spec try_add_nodes_to_passive_view(MapSet.t(Node.t()), t()) :: t()
  defp try_add_nodes_to_passive_view(remote_passive_view, state) do
    Enum.reduce(
      remote_passive_view,
      state,
      &try_add_node_to_passive_view(&1, &2)
    )
  end

  @spec try_add_node_to_passive_view(Node.t(), t()) :: t()
  defp try_add_node_to_passive_view(node, %State{passive_view_size: size} = state) do
    case MapSet.size(state.passive_view) do
      passive_size when passive_size >= size ->
        state
        |> trim_passive_view(size)
        |> add_node_to_passive_view(node)

      _ ->
        add_node_to_passive_view(state, node)
    end
  end

  @spec add_node_to_active_view(Node.t(), t()) :: t()
  defp add_node_to_active_view(node, state) do
    passive = MapSet.delete(state.passive_view, node)
    active = MapSet.put(state.active_view, node)
    %{state | passive_view: passive, active_view: active}
  end

  @spec add_node_to_passive_view(t(), Node.t()) :: t()
  defp add_node_to_passive_view(%{passive_view: passive, active_view: active} = state, node) do
    case {MapSet.member?(passive, node), MapSet.member?(active, node)} do
      {false, false} ->
        %{state | passive_view: MapSet.put(passive, node)}

      _ ->
        state
    end
  end

  @spec trim_passive_view(t(), pos_integer()) :: t()
  defp trim_passive_view(state, size) do
    state.passive_view
    |> Enum.take_random(state.passive_view_size - (size - 1))
    |> Enum.reduce(state, fn node, acc ->
      %{acc | passive_view: MapSet.delete(acc.passive_view, node)}
    end)
  end

  @spec join(t()) :: :ok | {:error, reason :: term()}
  defp join(state) do
    case select_node(state.passive_view) do
      {:ok, node} ->
        join(node, state)

      {:error, :nonode} = error ->
        error
    end
  end

  @spec join(Node.t(), t()) :: :ok | {:error, reason :: term()}
  defp join(node, _state) do
    case NodeMonitor.connect(node) do
      :ok ->
        send_join_message(node)

      {:error, _reason} = error ->
        error
    end
  end

  @spec neighbor(t()) :: {:ok, t()} | {:error, t()}
  defp neighbor(state) do
    case select_node(state.passive_view) do
      {:ok, node} ->
        state1 = neighbor(node, state)
        {:ok, state1}

      {:error, :nonode} ->
        {:error, :nonode}
    end
  end

  @spec neighbor(Node.t(), t()) :: t()
  defp neighbor(node, state) do
    case NodeMonitor.connect(node) do
      {:error, _} ->
        :ok = warn("Connect failed while sending NEIGHBOR, remove #{node} from passive view")
        _ = Process.send_after(self(), :sched_neighbor, state.neighbor_interval)
        %{state | passive_view: MapSet.delete(state.passive_view, node)}

      :ok ->
        priority = neighbor_priority(state)
        :ok = send_neighbor(node, priority)
        state
    end
  end

  @spec shuffle(t()) :: :ok
  defp shuffle(state) do
    case select_node(state.active_view) do
      {:ok, node} ->
        send_shuffle(
          node,
          Node.self(),
          state.arwl,
          state.passive_view,
          state.active_view,
          MapSet.new([Node.self()])
        )

      {:error, _} ->
        :ok
    end
  end

  @spec disconnect(Node.t(), t()) :: t()
  defp disconnect(node, state) do
    :ok = send_disconnect(node)
    Process.send_after(self(), {:sched_disconn, node}, :timer.seconds(3))
    passive = MapSet.put(state.passive_view, node)
    active = MapSet.delete(state.active_view, node)
    %{state | passive_view: passive, active_view: active}
  end

  @spec merge_passive_view(shuffle(), t()) :: t()
  defp merge_passive_view(shuffle, state) do
    state.passive_view
    |> MapSet.union(shuffle.passive_view)
    |> MapSet.union(shuffle.active_view)
    |> MapSet.delete(Node.self())
    |> try_add_nodes_to_passive_view(state)
  end

  @spec combine_with_remote_view(shuffle(), t()) :: MapSet.t(Node.t())
  defp combine_with_remote_view(shuffle, state) do
    state.passive_view
    |> MapSet.difference(shuffle.active_view)
    |> MapSet.difference(shuffle.passive_view)
    |> Enum.split(MapSet.size(shuffle.active_view))
    |> elem(0)
    |> MapSet.new()
  end

  @spec maybe_disconnect(Node.t(), t()) :: boolean()
  defp maybe_disconnect(node, state),
    do: maybe_disconnect_1(node, state)

  @spec maybe_disconnect_1(Node.t(), t()) :: :ok
  defp maybe_disconnect_1(node, state),
    do: MapSet.member?(state.active_view, node) || maybe_disconnect_2(node, state)

  @spec maybe_disconnect_2(Node.t(), t()) :: :ok
  defp maybe_disconnect_2(node, _state),
    do: if(Enum.member?(Node.list(), node), do: NodeMonitor.disconnect(node, 3000))

  @spec neighbor_priority(t()) :: :low | :high
  defp neighbor_priority(%State{active_view: active_view}),
    do: if(MapSet.size(active_view) > 0, do: :low, else: :high)

  @spec send_join_message(Node.t()) :: :ok
  defp send_join_message(node) do
    :ok = debug("Sending JOIN request to #{node}")

    send_message(
      node,
      %{
        msg: :join,
        sender: Node.self(),
        tref: Process.send_after(self(), {:join_timeout, node}, 1000)
      }
    )
  end

  @spec send_join_ack_message(Node.t(), reference(), t()) :: :ok
  defp send_join_ack_message(node, tref, state) do
    :ok = debug("Sending JOIN_ACK to #{node}")

    send_message(
      node,
      %{
        msg: :join_ack,
        sender: Node.self(),
        tref: tref,
        passive_view: state.passive_view
      }
    )
  end

  @spec bcast_forward_join(t(), Node.t()) :: :ok
  defp bcast_forward_join(state, new_node) do
    send_forward_join_message(
      MapSet.delete(state.active_view, new_node),
      new_node,
      state.arwl,
      MapSet.new([Node.self()]),
      state
    )
  end

  @spec send_forward_join_message(
          target :: Node.t(),
          new_node :: Node.t(),
          ttl :: non_neg_integer(),
          path :: MapSet.t(Node.t()),
          t()
        ) :: :ok
  defp send_forward_join_message(target, new_node, ttl, path, state) do
    :ok = debug("Sending FORWARD_JOIN to #{inspect(target)}")

    send_message(
      target,
      %{
        msg: :forward_join,
        sender: Node.self(),
        new_node: new_node,
        ttl: ttl,
        prwl: state.prwl,
        adv_path: path
      }
    )
  end

  @spec send_neighbor(Node.t(), prio :: :low | :high) :: :ok
  defp send_neighbor(node, prio) do
    :ok = debug("Sending NEIGHBOR to #{node}")

    send_message(
      node,
      %{
        msg: :neighbor,
        sender: Node.self(),
        priority: prio
      }
    )
  end

  @spec send_neighbor_ack(Node.t()) :: :ok
  defp send_neighbor_ack(node) do
    :ok = debug("Sending NEIGHBOR_ACK to #{node}")

    send_message(
      node,
      %{
        msg: :neighbor_ack,
        sender: Node.self()
      }
    )
  end

  @spec send_neighbor_nak(Node.t(), MapSet.t(Node.t())) :: :ok
  defp send_neighbor_nak(node, passive_view) do
    :ok = debug("Sending NEIGHBOR_NAK to #{node}")

    send_message(
      node,
      %{
        msg: :neighbor_nak,
        sender: Node.self(),
        passive_view: passive_view
      }
    )
  end

  @spec send_shuffle(
          Node.t(),
          Node.t(),
          non_neg_integer(),
          MapSet.t(Node.t()),
          MapSet.t(Node.t()),
          MapSet.t(Node.t())
        ) :: :ok
  defp send_shuffle(node, origin, ttl, passive_view, active_view, adv_path) do
    :ok = debug("Sending SHUFFLE to #{node}")

    send_message(
      node,
      %{
        msg: :shuffle,
        sender: Node.self(),
        origin: origin,
        ttl: ttl,
        passive_view: passive_view,
        active_view: active_view,
        adv_path: adv_path
      }
    )
  end

  @spec send_shuffle_reply(Node.t(), MapSet.t(Node.t())) :: :ok
  defp send_shuffle_reply(node, combined_view) do
    :ok = debug("Sending SHUFFLE_REPLY to #{node} with #{inspect(combined_view)}")

    send_message(
      node,
      %{
        msg: :shuffle_reply,
        sender: Node.self(),
        combined_view: combined_view
      }
    )
  end

  @spec send_disconnect(Node.t()) :: :ok
  defp send_disconnect(node) do
    send_message(
      node,
      %{
        msg: :disconnect,
        sender: Node.self()
      }
    )
  end

  @spec send_message(MapSet.t(Node.t()) | Node.t(), msg :: term()) :: :ok
  defp send_message(nodes, msg) when is_map(nodes),
    do: Enum.each(nodes, &send_message(&1, msg))

  defp send_message(node, msg) when is_atom(node),
    do: GenServer.cast({__MODULE__, node}, msg)

  @spec select_node(MapSet.t(Node.t())) :: {:ok, Node.t()} | {:error, :nonode}
  defp select_node(nodes) do
    case MapSet.size(nodes) do
      0 ->
        {:error, :nonode}

      len ->
        node = Enum.at(nodes, :rand.uniform(len) - 1)
        {:ok, node}
    end
  end

  @spec shuffle_interval(t()) :: non_neg_integer()
  defp shuffle_interval(state) do
    case MapSet.size(state.passive_view) do
      size when size <= 1 -> :timer.seconds(5)
      _ -> state.shuffle_interval
    end
  end

  @spec cancel_timer(reference() | any()) :: :ok
  defp cancel_timer(tref) do
    if is_reference(tref), do: Process.cancel_timer(tref)
    :ok
  end

  @spec notify_event(Node.t(), :up | :down, t()) :: :ok
  defp notify_event(node, evt, state),
    do:
      Enum.each(
        state.subscriber,
        &Process.send(&1, {:membership, node, evt}, [])
      )
end
