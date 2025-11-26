package main;

import (
	"fmt"
	"net"
	"os"
	"time"
	"log"
	"context"
	"sync"
	"replication-go/common"
	atm "replication-go/generic_atomic"
	proto "replication-go/grpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func ConnectToAuctionService(node common.NodeInfo) (proto.AuctionServiceClient, error) {
	conn, err := grpc.NewClient(node.ConnectionAddr, 
		grpc.WithTransportCredentials(insecure.NewCredentials()));

	if err != nil {
		return nil, err
	}

	serviceConn := proto.NewAuctionServiceClient(conn)

	_, err = serviceConn.Ping(context.Background(), &proto.Nothing{})
	if err != nil {
		return nil, err
	}

	return serviceConn, nil
}

type Bid struct {
	bidder string
	item string
	bid uint32
}

type Peer struct {
	info common.NodeInfo
	conn proto.AuctionServiceClient
	assumedDead bool
}

type AucServer struct {
	proto.UnimplementedAuctionServiceServer
	serverInfo common.NodeInfo
	peer Peer
	isLeader bool
	mu sync.Mutex

	auctionDeadline atm.GenericAtomic[time.Time]
	bid atm.GenericAtomic[Bid]
}

func NewAucServer(info common.NodeInfo, peer common.NodeInfo) *AucServer {
	server := new(AucServer);

	if info.ConnectionAddr == peer.ConnectionAddr {
		panic("Server addr and Peer addr cannot be the same")
	}

	server.isLeader = info.ConnectionAddr > peer.ConnectionAddr
	server.peer.info = peer
	server.peer.assumedDead = false

	server.serverInfo = info;

	server.bid.Store( Bid { bid: 0, item: "Generic item", bidder: "Nobody" } )
	server.auctionDeadline.Store( time.Now() )

	return server;
}

func (server *AucServer) Serve() {
	grpcServer := grpc.NewServer();
	proto.RegisterAuctionServiceServer(grpcServer, server);

	tcpConnection, err := net.Listen("tcp", server.serverInfo.ConnectionAddr);
	if err != nil {
		panic(fmt.Sprintf("Server failed to bind to connection addr %v", server.serverInfo.ConnectionAddr));
	}

	if server.isLeader {
		log.Println("Waiting for replica to connect")
		for {
			conn, err := ConnectToAuctionService(server.peer.info)
			if err != nil {
				continue
			}

			server.peer.conn = conn
			break
		}

		server.auctionDeadline.Store( time.Now().Add( 30*time.Second ) )

		server.peer.conn.ReplicateTime(context.Background(), 
			&proto.Time{ Timestamp: uint64(server.auctionDeadline.Load().Unix()) })
	} else {
		log.Println("Starting replica, expecting leader to already be running")
	}

	log.Println("Serving...")

	if !server.isLeader {
		go server.ReplicaFalloverHandler()
	}
	grpcServer.Serve(tcpConnection);
}

func (server *AucServer) ReplicaFalloverHandler() {

	conn, err := ConnectToAuctionService(server.peer.info)

	if err != nil {
		panic("Could not connect to leader, please start leader before replica")
	}

	server.peer.conn = conn

	for {
		time.Sleep(2*time.Second)
		log.Println("Checking if leader is still alive")

		ctx, cancel := context.WithTimeout(context.Background(), 2 * time.Second)
		_, err := server.peer.conn.Ping(ctx, &proto.Nothing{})
		cancel()

		if err != nil {
			log.Println("Assuming leader is dead, performing failover")
			server.isLeader = true
			server.peer.assumedDead = true
			break
		}
	}
}

func (server *AucServer) IsAuctionOver() bool {
	return server.auctionDeadline.Load().Before(time.Now());
}

func (server *AucServer) Result(
	ctx context.Context, req *proto.Nothing) (*proto.Outcome, error) {

	server.mu.Lock()
	defer server.mu.Unlock()

	log.Println("Got result request from client")

	if !server.isLeader {
		log.Println("This is the replica, ignoring request and returning error")
		out := proto.Outcome {IsValid: false}
		return &out, nil
	}

	bid := server.bid.Load()


	isAuctionOver := server.IsAuctionOver();

	log.Printf("Returning Bid {Amount: %d, Bidder: %s, IsFinalOutcome: %v}", bid.bid, bid.bidder, isAuctionOver)
	out := proto.Outcome {
		Bid: &proto.ClientBid { Username: bid.bidder, Amount: bid.bid, Item: bid.item  },
		IsFinal: isAuctionOver,
	}

	return &out, nil
}

func (server *AucServer) ReplicateTime(
	ctx context.Context, req *proto.Time) (*proto.Nothing, error) {
	log.Printf("Got time replication: %d", req.Timestamp)
	if server.isLeader {
		log.Printf("This is the leader, ignoring replication")
	} else {
		log.Printf("Updating own deadline")
		server.auctionDeadline.Store(
			time.Unix(int64(req.Timestamp), 0),)
	}

	return &proto.Nothing{}, nil
}

func (server *AucServer) Replicate(
	ctx context.Context, req *proto.ClientBid) (*proto.Nothing, error) {

	bid := Bid {
		bidder: req.Username,
		item: req.Item,
		bid: uint32(req.Amount),
	}

	log.Printf("Got replication for bid: %v", bid)

	if server.isLeader {
		log.Println("This is the leader, ignoring replication")
	} else {
		log.Println("Storing replication data")
		server.bid.Store(bid)
	}

	return &proto.Nothing{}, nil
}

func (server *AucServer) Bid(
	ctx context.Context, req *proto.ClientBid) (*proto.ClientBidResp, error) {

	server.mu.Lock()
	defer server.mu.Unlock()

	log.Printf("Got bid from: %s at %d for %s", req.Username, req.Amount, req.Item)

	if server.IsAuctionOver() {
		log.Println("Auction is over, ignoring bid")
		return &proto.ClientBidResp {Status: proto.ClientBidStatus_FAIL_AUCTION_IS_DONE}, nil
	}

	if !server.isLeader {
		log.Printf("This node is not the leader, ignoring bid and returning error")
		out := proto.ClientBidResp {Status: proto.ClientBidStatus_EXCEPTION}
		return &out, nil
	}

	bid := Bid {
		bidder: req.Username,
		item: req.Item,
		bid: uint32(req.Amount),
	}


	// Only accept bid if the bid is higher, if the bid item name matches and the current bid is still ongoing
	result := server.bid.ComparePredicateAndSwap(func (b Bid) bool {
		return b.bid < bid.bid && b.item == bid.item
	}, bid)

	out := proto.ClientBidResp {}

	if result {
		server.bid.Store(bid)
		log.Println("Bid accepted")
		out.Status = proto.ClientBidStatus_SUCCESS

		if !server.peer.assumedDead {
			_, err := server.peer.conn.Replicate(context.Background(), req)
			if err != nil {
				log.Println("Replica assumed dead")
				server.peer.assumedDead = true
			}
		}
	} else {
		log.Println("Bid rejected")
		out.Status = proto.ClientBidStatus_FAIL_BID_TOO_LOW
	}


	return &out, nil
}

func (server *AucServer) Ping(
	ctx context.Context, req *proto.Nothing) (*proto.IsLeader, error) {
	server.mu.Lock()
	defer server.mu.Unlock()

	if server.isLeader {
		log.Printf("Got a ping - informing pinger that this is the leader")
	} else {
		log.Printf("Got a ping - informing pinger that this is the replica")
	}

	out := proto.IsLeader{IsLeader: server.isLeader}

	return &out, nil
}

func main() {
	nodes, err := common.ReadServerFile("./servers.txt")
	if err != nil {
		panic(err.Error())
	}

	if len(nodes) != 2 {
		println("Expected only 2 nodes in servers file. First line for the server and second for the replica")
		return
	}

	leader := nodes[0]
	replica := nodes[1]

	if len(os.Args) < 2 {
		println("Running replica")
		server := NewAucServer(replica, leader)
		server.Serve()
	} else {
		if os.Args[1] != "leader" {
			println("Argument must be 'leader' to indicate that this is the leader node")
			return
		}
		println("Running leader")
		server := NewAucServer(leader, replica)
		server.Serve()
	}

}

