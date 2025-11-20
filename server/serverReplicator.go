package server;

import (
	"fmt"
	"net"
	"os"
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

type Bid struct {
	finalized bool
	bidder string
	item string
	bid uint32
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

	bids []Bid
}

func NewAucServer(info NodeInfo, leaderInfo *NodeInfo) *AucServer {
	server := new(AucServer);

	server.serverInfo = info;
	server.otherServers = make(map[NodeInfo]proto.AuctionServiceClient);
	server.messages = make(chan Message, 10);
	server.electionStatus.Store(NoElection);

	if leaderInfo == nil {
		server.isLeader.Store(true);
		server.leaderInfo = server.serverInfo;
	} else {
		server.isLeader.Store(false);
		server.leaderInfo = *leaderInfo;
	}

	return server;
}

func (server *AucServer) Serve() {
	grpcServer := grpc.NewServer();
	proto.RegisterAuctionServiceServer(grpcServer, server);

	tcpConnection, err := net.Listen("tcp", server.serverInfo.connectionAddr);

	if err != nil {
		panic(fmt.Sprintf("Server failed to bind to %s", server.serverInfo.connectionAddr));
	}

	go server.messageHandler();
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
	ctx context.Context, req *proto.ReplicationData) (*proto.Acknowledgement, error) {

	bid := Bid { bidder: req.Username, bid: req.Amount, item: req.Item }

	switch req.Kind {
	case proto.ReplicationEventKind_Bid:
		bid.finalized = false
		server.bids = append(server.bids, bid)
	case proto.ReplicationEventKind_Result:
		bid.finalized = true
		server.bids = append(server.bids, bid)
	}

	return &proto.Acknowledgement{}, nil
}

func (server *AucServer) AuctionHandler() {
	if !server.isLeader.Load() {
		return
	}
	var item = "a"

	for {
		data := proto.ReplicationData{ 
			Kind: proto.ReplicationEventKind_Bid,  
			Username: "nobody",
			Amount: 0,
			Item: item,
		};

		server.bids = append(server.bids, Bid{
			finalized: false,
			bidder: "nobody",
			item: item,
			bid: 0 })

		for _, node := range server.otherServers{
			node.Replicate(context.Background(), &data)
		}
	}

}

func main() {
	if len(os.Args) > 1 {
		server := NewAucServer(NodeInfo { "localhost:5001" }, &NodeInfo { "localhost:5000" })
		server.Serve()
	} else {
		server := NewAucServer(NodeInfo { "localhost:5000" }, nil)
		server.Serve()
	}
}

