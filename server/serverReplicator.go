package main;

import (
	"fmt"
	"net"
	"os"
	"log"
	"context"
	"sync/atomic"
	"replication-go/common"
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

	MakeBid
)

type Message struct {
	info common.NodeInfo
	kind MessageKind
	bid Bid
}

type ServerConnection struct {
	info common.NodeInfo
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
	serverInfo common.NodeInfo

	leaderInfo common.NodeInfo
	messages chan Message
	isLeader atomic.Bool
	electionStatus atm.GenericAtomic[ElectionState]

	otherServers map[common.NodeInfo]proto.AuctionServiceClient

	bid Bid
//	bids []Bid
}

func NewAucServer(info common.NodeInfo, leaderInfo *common.NodeInfo) *AucServer {
	server := new(AucServer);

	server.serverInfo = info;
	server.otherServers = make(map[common.NodeInfo]proto.AuctionServiceClient);
	server.messages = make(chan Message, 10);
	server.electionStatus.Store(NoElection);

	server.bid.bid = 0;
	server.bid.item = "Generic Item";
	server.bid.bidder = "Nobody";
	server.bid.finalized = false;

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

	tcpConnection, err := net.Listen("tcp", server.serverInfo.Get());

	if err != nil {
		panic(fmt.Sprintf("Server failed to bind to %s", server.serverInfo.Get()));
	}

	go server.messageHandler();
	grpcServer.Serve(tcpConnection);
}

func (server *AucServer) HoldElection(
	ctx context.Context,
	req *proto.NodeInfo) (*proto.Acknowledgement, error) {

	msg := Message { info: common.NodeInfo { ConnectionAddr: req.ConnectionAddr }, kind: HoldElection };
	server.messages <- msg

	return &proto.Acknowledgement{}, nil
}

// 4
func (server *AucServer) HoldElectionHandler(otherInfo common.NodeInfo) error {

	protoMessage := proto.NodeInfo{ConnectionAddr: server.serverInfo.Get()};
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
	otherInfo := common.NodeInfoFromReq(req);
	server.messages <- Message { info: otherInfo, kind: EnterElection }

	return &proto.Acknowledgement{}, nil
}

func (server *AucServer) EnterElectionHandler(otherInfo common.NodeInfo) error {
	if server.serverInfo.LessThan(&otherInfo) {
		server.electionStatus.Store(WaitingForVictoryMessage);
	}

	return nil
}

func (server *AucServer) ElectionWinner(
	ctx context.Context,
	req *proto.NodeInfo) (*proto.Acknowledgement, error) {

	otherInfo := common.NodeInfoFromReq(req);
	server.messages <- Message { info: otherInfo, kind: Winner }

	return &proto.Acknowledgement{}, nil
}

func (server *AucServer) ElectionWinnerHandler(otherInfo common.NodeInfo) error {
	server.leaderInfo = otherInfo;
	server.isLeader.Store(false);
	return nil
}

func (server *AucServer) WhoIsTheLeader(
	ctx context.Context, req *proto.Nothing) (*proto.NodeInfo, error) {

	// Wait for ongoing election to finish before replying
	for server.electionStatus.Load() != NoElection {
		
	}

	return &proto.NodeInfo{ ConnectionAddr: server.leaderInfo.ConnectionAddr }, nil
}


func (server *AucServer) messageHandler() {
	for {
		msg := <- server.messages
		log.Println("Read message")

		switch msg.kind {
		case HoldElection:
			server.HoldElectionHandler(msg.info)
		case EnterElection:
			server.EnterElectionHandler(msg.info)
		case Winner:
			server.ElectionWinnerHandler(msg.info)

		case MakeBid:
			if !server.isLeader.Load() {
				log.Println("Got a bid, but I'm not the leader")
				continue
			}
			server.BidHandler(msg.bid)
		}
	}
}

func (server *AucServer) Replicate(
	ctx context.Context, req *proto.ReplicationData) (*proto.Acknowledgement, error) {

	if server.isLeader.Load() {
		log.Println("Possible split brain error occured - Received replication data from leader whilst being the leader")
		return &proto.Acknowledgement{}, nil
	}

	bid := Bid { bidder: req.Username, bid: req.Amount, item: req.Item }

	switch req.Kind {
	case proto.ReplicationEventKind_Bid:
		bid.finalized = false
//		server.bids = append(server.bids, bid)
	case proto.ReplicationEventKind_Result:
		bid.finalized = true
//		server.bids = append(server.bids, bid)
	}

	server.bid = bid

	return &proto.Acknowledgement{}, nil
}

func (server *AucServer) Bid(
	ctx context.Context, req *proto.ClientBid) (*proto.Acknowledgement, error) {

	if !server.isLeader.Load() {
		log.Println("Non leader got Bid")
		return &proto.Acknowledgement{}, nil
	}

	server.messages <- Message {kind: MakeBid, bid: Bid {
		finalized: false,
		bidder: req.Username,
		item: req.Item,
		bid: uint32(req.Amount),
	}}

	return &proto.Acknowledgement{}, nil
}

func (server *AucServer) BidHandler(bid Bid) error {
	log.Printf("Got bid from: %s at %d for %s", bid.bidder, bid.bid, bid.item)
	server.bids = append(server.bids, bid)

	if server.isLeader.Load() {
		data := proto.ReplicationData {
			Kind: proto.ReplicationEventKind_Bid,
			Username: bid.bidder,
			Amount: bid.bid,
			Item: bid.item,
		} 
		for _, conn := range server.otherServers {
			conn.Replicate(context.Background(), &data)
		}
	}
	return nil
}

func main() {
	if len(os.Args) > 1 {
		server := NewAucServer(common.NodeInfo { ConnectionAddr: "localhost:5001" }, &common.NodeInfo { ConnectionAddr: "localhost:5000" })
		server.Serve()
	} else {
		server := NewAucServer(common.NodeInfo { ConnectionAddr: "localhost:5000" }, nil)
		server.Serve()
	}
}

