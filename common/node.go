package common

import (
	proto "replication-go/grpc"
)

type NodeInfo struct {
	ConnectionAddr string
}

func NodeInfoFromReq(req *proto.NodeInfo) NodeInfo {
	return NodeInfo {
		req.ConnectionAddr,
	};
}

func (this *NodeInfo) LessThan(that *NodeInfo) bool {
	return this.ConnectionAddr < that.ConnectionAddr;
}

func (this *NodeInfo) Get() string {
	return this.ConnectionAddr;
}

