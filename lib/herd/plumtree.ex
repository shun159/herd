defmodule Herd.Plumtree do
  @moduledoc false

  use Supervisor

  @plumtree_manager %{
    id: Herd.Plumtree.Manager,
    start: {Herd.Plumtree.Manager, :start_link, []},
    restart: :permanent,
    shutdown: 5000,
    type: :worker,
    modules: [Herd.Plumtree.Manager]
  }

  @children [
    @plumtree_manager
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
