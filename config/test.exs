import Config

config :herd,
  contact_nodes: [
    :"cluster1@127.0.0.1",
    :"cluster2@127.0.0.1",
    :"cluster3@127.0.0.1"
  ]

config :aten,
  detection_threshold: 1,
  poll_interval: 5000
