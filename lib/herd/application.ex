defmodule Herd.Application do
  @moduledoc false

  use Application

  def start(_type, _args) do
    {:ok, _pid} = Herd.Supervisor.start_link()
  end
end
