package main;

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"log"
	"context"
	"google.golang.org/grpc/credentials/insecure"
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

	NewFollower

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
	grpcConn *grpc.ClientConn
	assumedDead bool
}

func (serverConnection *ServerConnection) AssumedDead() bool {
	return serverConnection.grpcConn == nil || serverConnection.conn == nil || serverConnection.assumedDead
}

func (serverConnection *ServerConnection) Close() {
	if serverConnection.grpcConn != nil {
		serverConnection.grpcConn.Close()
	}
}

func (serverConnection *ServerConnection) Reconnect() bool {
	if !serverConnection.AssumedDead() {
		serverConnection.grpcConn.Close();
	}
	serverConnection.assumedDead = true

	conn, err := grpc.NewClient(serverConnection.info.ConnectionAddr, 
		grpc.WithTransportCredentials(insecure.NewCredentials()));

	if err != nil {
		log.Printf("Error connecting: %s", err.Error())
		return false
	}

	serverConnection.grpcConn = conn
	serverConnection.conn = proto.NewAuctionServiceClient(serverConnection.grpcConn)

	// check for pulse
	_, err = serverConnection.conn.WhoIsTheLeader(context.Background(), &proto.Nothing{})

	if err != nil {
		log.Printf("Error checking server pulse: %s", err.Error())
		return false
	}

	serverConnection.assumedDead = false

	return true
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

	otherServers map[common.NodeInfo]ServerConnection

	bid atm.GenericAtomic[Bid]
//	bids []Bid
}

func NewAucServer(id uint, nodes []common.NodeInfo) *AucServer {
	server := new(AucServer);

	if int(id) >= len(nodes) {
		panic(fmt.Sprintf("Index out of bounds: %d for %v", id, nodes))
	}

	server.serverInfo = nodes[id];
	server.otherServers = make(map[common.NodeInfo]ServerConnection, len(nodes));
	server.messages = make(chan Message, 10);
	server.electionStatus.Store(NoElection);

	for i, v := range nodes {
		if uint(i) == id {
			continue
		}

		server.otherServers[v] = ServerConnection {
			info: common.NodeInfo { ConnectionAddr: v.ConnectionAddr },
			assumedDead: true,
		}
	}

	for i, v := range server.otherServers {
		log.Printf("Attempting to connect to %v", i)

		if v.Reconnect() {
			log.Printf("Connection to %v successful, sending new follower message", i)
			v.conn.NewFollower(context.Background(), &proto.NodeInfo{ConnectionAddr: server.serverInfo.ConnectionAddr})

			leaderInfo, err := v.conn.WhoIsTheLeader(context.Background(), &proto.Nothing{})
			if err != nil {
				v.assumedDead = true
				log.Printf("Connection to %v failed after sending message", i)
				continue
			}

			server.leaderInfo.ConnectionAddr = leaderInfo.ConnectionAddr
		}
	}

	server.bid.Store( Bid { bid: 0, item: "Generic item", bidder: "Nobody", finalized: false } )

	server.isLeader.Store(false);

	return server;
}

func (server *AucServer) Serve(id uint, nodes []common.NodeInfo) {
	grpcServer := grpc.NewServer();
	proto.RegisterAuctionServiceServer(grpcServer, server);

	var err error
	var tcpConnection net.Listener
	for _, node := range nodes {
		tcpConnection, err = net.Listen("tcp", node.ConnectionAddr);
		if err == nil {
			break
		}
	}
	if err != nil {
		panic(fmt.Sprintf("Server failed to bind to any connection addr %v", nodes));
	}

	go server.messageHandler();
	go server.bullyHandler();
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
	log.Printf("Got hold election message from %s", otherInfo.ConnectionAddr)

	protoMessage := proto.NodeInfo{ConnectionAddr: server.serverInfo.Get()};
	if otherInfo.LessThan(&server.serverInfo) {

		server.otherServers[otherInfo].conn.EnterElection(
			context.Background(), &protoMessage,
		);

		// If not already running election, then set running election to true
		// and run the following block of code
		if server.electionStatus.CompareAndSwap(NoElection, RunningElection) {
			for info, conn := range server.otherServers {
				if server.serverInfo.LessThan(&info) {
					if conn.AssumedDead() {
						continue
					}
					_, err := conn.conn.HoldElection(context.Background(), &protoMessage);
					if err != nil {
						conn.assumedDead = true
					}
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
	log.Printf("Got enter election message from %s", otherInfo.ConnectionAddr)
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
	log.Printf("Got election winner message from %s", otherInfo.ConnectionAddr)
	server.leaderInfo = otherInfo;
	server.isLeader.Store(false);
	server.electionStatus.Store(NoElection)
	return nil
}

func (server *AucServer) WhoIsTheLeader(
	ctx context.Context, req *proto.Nothing) (*proto.NodeInfo, error) {

	// Wait for ongoing election to finish before replying
	for server.electionStatus.Load() != NoElection {
		
	}

	return &proto.NodeInfo{ ConnectionAddr: server.leaderInfo.ConnectionAddr }, nil
}

func (server *AucServer) NewFollower(
	ctx context.Context,
	req *proto.NodeInfo) (*proto.Acknowledgement, error) {

	otherInfo := common.NodeInfoFromReq(req);

	msg := Message{ info: otherInfo, kind: NewFollower }

	server.messages <- msg

	return &proto.Acknowledgement{}, nil
}

func (server *AucServer) bullyHandler() {
	selfInfo := proto.NodeInfo { ConnectionAddr: server.serverInfo.ConnectionAddr }

	for {

		if server.electionStatus.Load() != NoElection {
			continue
		}

		if server.isLeader.Load() {
			continue
		}

		if server.leaderInfo.ConnectionAddr != "" {
			srv, found := server.otherServers[server.leaderInfo]
			if found && !srv.AssumedDead() {
				continue
			}
		}

		log.Println("Leader assumed dead, holding election")
		server.electionStatus.CompareAndSwap(NoElection, RunningElection)

		var greatest = true
		for _, srv := range server.otherServers {
			if !srv.Reconnect() {
				continue
			}

			_, err := srv.conn.HoldElection(context.Background(), &selfInfo)
			if err != nil { 
				srv.assumedDead = true;
			} else if server.leaderInfo.ConnectionAddr < srv.info.ConnectionAddr  {
				greatest = false
			}
		}

		if greatest {
			log.Println("Greatest server ID still alive, declaring self as leader")
			server.electionStatus.Store(NoElection)
			for _, srv := range server.otherServers {
				if srv.AssumedDead() {
					continue
				}
				srv.conn.ElectionWinner(context.Background(), &selfInfo)
			}
			server.isLeader.Store(true)
			server.leaderInfo = server.serverInfo
		}
	}
}

func (server *AucServer) NewFollowerHandler(info common.NodeInfo) {
	log.Printf("Got new follower request from %v", info)
	srv, found := server.otherServers[info]

	if !found {
		log.Printf("The follower request came from an unknown node %v", info)
	}

	srv.Reconnect()
}

func (server *AucServer) messageHandler() {
	log.Printf("Starting message handler on %v", server.serverInfo)
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

		case NewFollower:
			server.NewFollowerHandler(msg.info)

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

	server.bid.Store(bid)

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
	//server.bids = append(server.bids, bid)

	if server.isLeader.Load() {
		log.Println("Leader received the aforementioned bid")
		if !server.bid.ComparePredicateAndSwap(func (b Bid) bool { return b.bid < bid.bid; }, bid) {
			log.Printf("The bid is lower that the current, ignoring it")
			return nil
		}

		data := proto.ReplicationData {
			Kind: proto.ReplicationEventKind_Bid,
			Username: bid.bidder,
			Amount: bid.bid,
			Item: bid.item,
		} 
		for _, conn := range server.otherServers {
			if conn.AssumedDead() {
				continue
			}
			_, err := conn.conn.Replicate(context.Background(), &data)
			if err != nil {
				conn.assumedDead = true
			}
		}
	}
	return nil
}

func main() {
	nodes, err := common.ReadServerFile("./servers.txt")
	if err != nil {
		panic(err.Error())
	}

	if len(os.Args) < 2 {
		println("Must provide 0-indexed id which refers to a line in './servers.txt'")
		return
	}

	id, err := strconv.Atoi(os.Args[1])
	if id < 0 || id >= len(nodes) {
		fmt.Printf("ID out of bounds: %d for %v", id, nodes)
		return
	}

	server := NewAucServer(uint(id), nodes)
	server.Serve(uint(id), nodes)
}

