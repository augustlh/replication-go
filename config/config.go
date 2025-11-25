package config

type NodeConfig struct {
	ID   int64
	Addr string
}

var Nodes = []NodeConfig{
	{ID: 1, Addr: "localhost:5001"},
	{ID: 2, Addr: "localhost:5002"},
}
