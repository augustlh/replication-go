package server;

import (
	"fmt"
	"net"
	"context"
	"sync/atomic"
	atm "replication-go/generic_atomic"
	proto "replication-go/grpc"
	"google.golang.org/grpc"
	_ "google.golang.org/grpc/credentials/insecure"
)

type MessageKind byte;
const (
	HoldElection MessageKind = iota
	EnterElection
	Winner
)

type Message struct {
	info NodeInfo
	kind MessageKind
}

type NodeInfo struct {
	connectionAddr string
}

func NodeInfoFromReq(req *proto.NodeInfo) NodeInfo {
	return NodeInfo {
		req.ConnectionAddr,
	};
}

func (this *NodeInfo) LessThan(that *NodeInfo) bool {
	return this.connectionAddr < that.connectionAddr;
}

type ServerConnection struct {
	info NodeInfo
	conn proto.AuctionServiceClient
	assumedDead bool
}

type ElectionState byte;
const (
	NoElection ElectionState = iota
	RunningElection
	WaitingForVictoryMessage
)

type AucServer struct {
	proto.UnimplementedAuctionServiceServer
	serverInfo NodeInfo

	messages chan Message
	isLeader atomic.Bool
	electionStatus atm.GenericAtomic[ElectionState]

	otherServers map[NodeInfo]proto.AuctionServiceClient
}

func NewAucServer(info NodeInfo) *AucServer {
	server := new(AucServer);

	server.serverInfo = info;
	server.otherServers = make(map[NodeInfo]proto.AuctionServiceClient);
	server.messages = make(chan Message, 10);
	server.isLeader.Store(true);
	server.electionStatus.Store(NoElection);

	return server;
}

func (server *AucServer) Serve() {

	grpcServer := grpc.NewServer();
	proto.RegisterAuctionServiceServer(grpcServer, server);

	tcpConnection, err := net.Listen("tcp", server.serverInfo.connectionAddr);

	if err != nil {
		panic(fmt.Sprintf("Server failed to bind to %s", server.serverInfo.connectionAddr));
	}

	grpcServer.Serve(tcpConnection);
}

// 4
func (server *AucServer) HoldElection(
	ctx context.Context,
	req *proto.NodeInfo) (*proto.Acknowledgement, error) {

	protoMessage := proto.NodeInfo{ConnectionAddr: server.serverInfo.connectionAddr};
	otherInfo := NodeInfoFromReq(req);
	if otherInfo.LessThan(&server.serverInfo) {

		server.otherServers[otherInfo].EnterElection(
			ctx, &protoMessage,
		);

		// If not already running election, then set running election to true
		// and run the following block of code
		if server.electionStatus.CompareAndSwap(NoElection, RunningElection) {
			for info, conn := range server.otherServers {
				if server.serverInfo.LessThan(&info) {
					conn.HoldElection(context.Background(), &protoMessage);
				}
			}
		}

	}

	return &proto.Acknowledgement{}, nil
}

// 3
func (server *AucServer) EnterElection(ctx context.Context, req *proto.NodeInfo) (*proto.Nothing, error) {
	otherInfo := NodeInfoFromReq(req);
	if server.serverInfo.LessThan(&otherInfo) {
		server.electionStatus.Store(WaitingForVictoryMessage);
	}

	return &proto.Nothing{}, nil
}

func (server *AucServer) ElectionWinner(
	ctx context.Context,
	req *proto.NodeInfo) (*proto.Acknowledgement, error) {
	server.isLeader.Store(false);

	return &proto.Acknowledgement{}, nil
}

func (server *AucServer) Replicate(ctx context.Context, req *proto.Nothing) (*proto.Nothing, error) {
	return &proto.Nothing{}, nil
}


func (server *AucServer) HoldElection2() {

	for _, conn := range server.otherServers {
		conn.HoldElection(context.Background(), &proto.Nothing{});
	}

}

