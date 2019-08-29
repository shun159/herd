defmodule Herd.Hyparview.Membership.Test do
  use ExUnit.Case, async: false

  setup_all do
    nodes = LocalCluster.start_nodes("cluster", 2)
    :ok = Process.sleep(30_000)
    {:ok, nodes: nodes}
  end

  describe "Herd.Hyparview.Membership.get_active_view/0" do
    test "with 10 cluster members", %{nodes: _nodes} do
      Herd.Hyparview.Membership.get_active_view()
      |> elem(1)
      |> Enum.each(fn node ->
        {Herd.Hyparview.Membership, node}
        |> GenServer.call(:get_active_view)
        |> elem(1)
        |> Enum.member?(Node.self())
        |> assert()
      end)
    end
  end
end
