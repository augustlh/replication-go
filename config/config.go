package config

type NodeConfig struct {
	ID   int64
	Addr string
}

var Nodes = []NodeConfig{
	{ID: 1, Addr: "localhost:50051"},
	{ID: 2, Addr: "localhost:50052"},
	{ID: 3, Addr: "localhost:50053"},
}
