defmodule Herd.Supervisor do
  @moduledoc false

  use Supervisor

  @node_monitor %{
    id: Herd.Hyparview.NodeMonitor,
    start: {Herd.Hyparview.NodeMonitor, :start_link, []},
    restart: :permanent,
    shutdown: 5000,
    type: :worker,
    modules: [Herd.Hyparview.NodeMonitor]
  }

  @membership %{
    id: Herd.Hyparview.Membership,
    start: {Herd.Hyparview.Membership, :start_link, []},
    restart: :permanent,
    shutdown: 5000,
    type: :worker,
    modules: [Herd.Hyparview.Membership]
  }

  @plumtree_sup %{
    id: Herd.Plumtree,
    start: {Herd.Plumtree, :start_link, []},
    restart: :permanent,
    type: :supervisor,
    modules: [Herd.Plumtree]
  }

  @children [
    @node_monitor,
    @membership,
    @plumtree_sup
  ]

  @sup_flags [
    strategy: :one_for_all,
    max_restarts: 5,
    max_seconds: 10
  ]

  def start_link do
    Supervisor.start_link(__MODULE__, [], name: __MODULE__)
  end

  def init(_args) do
    Supervisor.init(@children, @sup_flags)
  end
end
