package main;

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"log"
	"context"
	"replication-go/common"
	atm "replication-go/generic_atomic"
	proto "replication-go/grpc"
	"google.golang.org/grpc"
	_ "google.golang.org/grpc/credentials/insecure"
)

type Bid struct {
	finalized bool
	bidder string
	item string
	bid uint32
}

type AucServer struct {
	proto.UnimplementedAuctionServiceServer
	serverInfo common.NodeInfo

	bids chan Bid
	bid atm.GenericAtomic[Bid]
}

func NewAucServer(id uint, nodes []common.NodeInfo) *AucServer {
	server := new(AucServer);

	if int(id) >= len(nodes) {
		panic(fmt.Sprintf("Index out of bounds: %d for %v", id, nodes))
	}

	server.serverInfo = nodes[id];
	server.bids = make(chan Bid, 10);

	server.bid.Store( Bid { bid: 0, item: "Generic item", bidder: "Nobody", finalized: false } )

	return server;
}

func (server *AucServer) Serve() {
	grpcServer := grpc.NewServer();
	proto.RegisterAuctionServiceServer(grpcServer, server);

	tcpConnection, err := net.Listen("tcp", server.serverInfo.ConnectionAddr);
	if err != nil {
		panic(fmt.Sprintf("Server failed to bind to connection addr %v", server.serverInfo.ConnectionAddr));
	}

	go server.messageHandler();
	grpcServer.Serve(tcpConnection);
}

func (server *AucServer) messageHandler() {
	log.Printf("Starting message handler on %v", server.serverInfo)
	for {
		bid := <- server.bids
		log.Println("Read bid: ", bid)
		server.BidHandler(bid)
	}
}

func (server *AucServer) Result(
	ctx context.Context, req *proto.Nothing) (*proto.Outcome, error) {

	bid := server.bid.Load()

	out := proto.Outcome {
		Bid: &proto.ClientBid { Username: bid.bidder, Amount: bid.bid, Item: bid.item  },
		IsFinal: false,
	}

	return &out, nil
}

func (server *AucServer) Bid(
	ctx context.Context, req *proto.ClientBid) (*proto.Acknowledgement, error) {

	server.bids <- Bid {
		finalized: false,
		bidder: req.Username,
		item: req.Item,
		bid: uint32(req.Amount),
	}

	return &proto.Acknowledgement{}, nil
}

func (server *AucServer) Ping(
	ctx context.Context, req *proto.Nothing) (*proto.Nothing, error) {

	return &proto.Nothing{}, nil
}

func (server *AucServer) BidHandler(bid Bid) error {
	log.Printf("Got bid from: %s at %d for %s", bid.bidder, bid.bid, bid.item)

	result := server.bid.ComparePredicateAndSwap(func (b Bid) bool {
		return b.bid < bid.bid && b.item == bid.item
	}, bid)

	if result {
		log.Println("Bid accepted")
	} else {
		log.Println("Bid rejected")
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
	server.Serve()
}

