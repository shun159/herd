defmodule Herd.MixProject do
  use Mix.Project

  def project do
    [
      app: :herd,
      version: "0.1.0",
      elixir: "~> 1.9",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [
      extra_applications: [:logger],
      mod: {Herd.Application, []}
    ]
  end

  defp deps do
    [
      {:aten, github: "shun159/aten", branch: "master"},
      # Code Quality
      {:credo, "~> 1.1.3", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.0.0-rc.6", only: [:dev], runtime: false},
      # Document
      {:earmark, "~> 1.3.5", only: :doc, runtime: false},
      {:ex_doc, "~> 0.21.1", only: :doc, runtime: false},
      # Test
      {:local_cluster, "~> 1.1", only: [:test]}
    ]
  end
end
