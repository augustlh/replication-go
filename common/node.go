package common

import (
	proto "replication-go/grpc"
	"os"
	"strings"
	"strconv"
)
 
func Map[x any, y any](xs []x, f func(x) y) []y {
    out := make([]y, len(xs))

    for i, v := range xs {
        out[i] = f(v)
    }
    return out
}

func Filter[x any](xs []x, predicate func(x) bool) []x {
    out := make([]x, len(xs))

	var i = 0
    for _, v := range xs {
		if predicate(v) {
			out[i] = v
			i++
		}
    }

	return out[:i]
}

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

func ipIsValid(ip string) bool {
	if len(ip) == 0 {
		return false
	}

	if ip == "localhost" {
		return true
	}

	numbers := strings.Split(ip, ".")

	if len(numbers) != 4 {
		return false
	}

	for _, v := range numbers {
		v, err := strconv.Atoi(v)
		if err != nil {
			return false
		}
		if v < 0 || v > 256 {
			return false
		}
	}

	return true
}

func connectionAddrIsValid(addr string) bool {
	strs := strings.SplitN(addr, ":", 2)
	if strs[0] != "localhost" && !ipIsValid(strs[0]) {
		return false
	}

	port, err := strconv.Atoi(strs[1])
	if err != nil {
		return false
	}

	return port >= 1 && port <= 65536
}

func ReadServerFile(filepath string) ([]NodeInfo, error) {
	bytes, err := os.ReadFile(filepath)

	if err != nil {
		return nil, err
	}

	servers := Map(
		Filter(strings.Split(string(bytes), "\n"),
			func (x string) bool { return connectionAddrIsValid(x); }),
		func (x string) NodeInfo { return NodeInfo { ConnectionAddr: x }},
	)

	return servers, nil
}

