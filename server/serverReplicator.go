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

	leaderInfo NodeInfo
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
	server.leaderInfo = server.serverInfo;
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

func (server *AucServer) HoldElection(
	ctx context.Context,
	req *proto.NodeInfo) (*proto.Acknowledgement, error) {

	msg := Message { info: NodeInfo { req.ConnectionAddr }, kind: HoldElection };
	server.messages <- msg

	return &proto.Acknowledgement{}, nil
}

// 4
func (server *AucServer) HoldElectionHandler(otherInfo NodeInfo) error {

	protoMessage := proto.NodeInfo{ConnectionAddr: server.serverInfo.connectionAddr};
	if otherInfo.LessThan(&server.serverInfo) {

		server.otherServers[otherInfo].EnterElection(
			context.Background(), &protoMessage,
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

	return nil
}

// 3
func (server *AucServer) EnterElection(
	ctx context.Context, req *proto.NodeInfo) (*proto.Acknowledgement, error) {
	otherInfo := NodeInfoFromReq(req);
	server.messages <- Message { info: otherInfo, kind: EnterElection }

	return &proto.Acknowledgement{}, nil
}

func (server *AucServer) EnterElectionHandler(otherInfo NodeInfo) error {
	if server.serverInfo.LessThan(&otherInfo) {
		server.electionStatus.Store(WaitingForVictoryMessage);
	}

	return nil
}

func (server *AucServer) ElectionWinner(
	ctx context.Context,
	req *proto.NodeInfo) (*proto.Acknowledgement, error) {

	otherInfo := NodeInfoFromReq(req);
	server.messages <- Message { info: otherInfo, kind: Winner }

	return &proto.Acknowledgement{}, nil
}

func (server *AucServer) ElectionWinnerHandler(otherInfo NodeInfo) error {
	server.leaderInfo = otherInfo;
	server.isLeader.Store(false);
	return nil
}

func (server *AucServer) messageHandler() {
	for {
		msg := <- server.messages

		switch msg.kind {
		case HoldElection:
		server.HoldElectionHandler(msg.info)
		case EnterElection:
		server.EnterElectionHandler(msg.info)
		case Winner:
		server.ElectionWinnerHandler(msg.info)
		}
	}
}

func (server *AucServer) Replicate(
	ctx context.Context, req *proto.Nothing) (*proto.Nothing, error) {
	return &proto.Nothing{}, nil
}



