package server

import (
	as "auction_system/proto"
	"fmt"
	"os"
	"slices"
	"strconv"

	"context"
	"log"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type AuctionState struct {
	RegisteredBidders []string
	HighestBid        int64
	HighestBidder     string
	Closed            bool
	Deadline          time.Time
}

type ClusterState struct {
	isLeader bool
	leaderID int64
}

type Peer struct {
	id              int64
	nodeClient      as.NodeServiceClient
	auctionClient   as.AuctionServiceClient
	isSuspectedDead bool
}

type Node struct {
	as.UnimplementedAuctionServiceServer
	as.UnimplementedNodeServiceServer

	id   int64
	addr string

	mu      sync.RWMutex
	auction *AuctionState
	cluster ClusterState
	peer    *Peer
}

func NewNode(id int64, addr string, peerId int64, peerAddr string) *Node {
	n := &Node{
		id:   id,
		addr: addr,
		auction: &AuctionState{
			RegisteredBidders: []string{},
			HighestBid:        0,
			HighestBidder:     "",
			Closed:            false,
			Deadline:          time.Now().Add(5 * time.Minute),
		},
		cluster: ClusterState{},
		peer:    nil,
	}

	leaderID := max(peerId, id)
	n.cluster.leaderID = leaderID
	n.cluster.isLeader = id == leaderID

	conn, err := grpc.NewClient(peerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(err)
	}
	n.peer = &Peer{
		id:            peerId,
		auctionClient: as.NewAuctionServiceClient(conn),
		nodeClient:    as.NewNodeServiceClient(conn),
	}
	return n
}

func (n *Node) ReplicateRequest(ctx context.Context, req *as.PlaceBidReq) (*emptypb.Empty, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	log.Printf("Was asked to replicate request '%v'", &req)

	n.HandleBid(req)
	log.Printf("Finished replicating request '%v'", &req)

	return &emptypb.Empty{}, nil
}

func (n *Node) HandleBid(req *as.PlaceBidReq) *as.PlaceBidResp {
	// You must hold the lock when entering this function, otherwise its not blocking!
	log.Printf("Handling bid '%v'", &req)
	now := time.Now()

	if n.auction.Closed || now.After(n.auction.Deadline) {
		n.auction.Closed = true
		log.Printf("Bid '%v' was rejected because the auction is over.", &req)
		return &as.PlaceBidResp{Accepted: false, Reason: "auction closed"}
	}

	if !slices.Contains(n.auction.RegisteredBidders, req.BidderId) {
		log.Printf("Bid '%v' was from a new client, adding to RegisteredBidders", &req)
		n.auction.RegisteredBidders = append(n.auction.RegisteredBidders, req.BidderId)
	}

	if req.Amount <= n.auction.HighestBid {
		log.Printf("Bid '%v' was rejected with reason: bid too low", &req)
		return &as.PlaceBidResp{Accepted: false, Reason: "bid too low"}
	}

	n.auction.HighestBid = req.Amount
	n.auction.HighestBidder = req.BidderId

	log.Printf("Bid '%v' was accepted", &req)
	return &as.PlaceBidResp{Accepted: true, Reason: ""}

}

func (n *Node) PlaceBid(ctx context.Context, req *as.PlaceBidReq) (*as.PlaceBidResp, error) {
	n.mu.RLock()
	isLeader := n.cluster.isLeader
	leaderID := n.cluster.leaderID
	n.mu.RUnlock()

	if !isLeader {
		if leaderID != n.peer.id {
			log.Printf("Received bid request '%v' while no leader was available!", &req)
			return &as.PlaceBidResp{Accepted: false, Reason: "no leader available"}, nil
		}
		log.Printf("Received bid request '%v' redirecting to leader!", &req)
		return n.peer.auctionClient.PlaceBid(ctx, req)
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	log.Printf("Replicating request request '%v'", &req)
	_, err := n.peer.nodeClient.ReplicateRequest(context.Background(), req)
	if err != nil {
		log.Printf("It seems my replica has crashed :(")
	}

	res := n.HandleBid(req)

	return res, nil
}

func (n *Node) GetStatus(ctx context.Context, _ *emptypb.Empty) (*as.GetStatusResp, error) {
	log.Printf("Received get status request")
	n.mu.RLock()
	isLeader := n.cluster.isLeader
	leaderID := n.cluster.leaderID
	n.mu.RUnlock()

	if !isLeader {
		if leaderID != n.peer.id {
			log.Printf("Auction is closed")
			return &as.GetStatusResp{Closed: true}, nil
		}

		log.Printf("Redirecting status request to leader")
		return n.peer.auctionClient.GetStatus(ctx, &emptypb.Empty{})
	}

	n.mu.RLock()
	defer n.mu.RUnlock()

	log.Printf("Sending status response")
	closed := n.auction.Closed || time.Now().After(n.auction.Deadline)

	return &as.GetStatusResp{
		Closed:            closed,
		HighestBid:        n.auction.HighestBid,
		HighestBidder:     n.auction.HighestBidder,
		RegisteredBidders: append([]string(nil), n.auction.RegisteredBidders...),
		Deadline:          timestamppb.New(n.auction.Deadline),
	}, nil
}

func (n *Node) startHeartbeatLoop(heartbeatInterval time.Duration, heartbeatTimeout time.Duration) {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()
	for range ticker.C {
		n.mu.RLock()
		isLeader := n.cluster.isLeader
		peer := n.peer
		n.mu.RUnlock()

		if isLeader {
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), heartbeatTimeout)
		_, err := peer.nodeClient.Heartbeat(ctx, &as.PingRequest{FromId: n.id})
		cancel()

		if err != nil {
			n.peer.isSuspectedDead = true
			n.handleLeaderSuspected()
			continue
		}
	}
}

func (n *Node) handleLeaderSuspected() {
	n.mu.Lock()
	defer n.mu.Unlock()

	log.Printf("An election has been called. Running election...")

	if n.id > n.peer.id || n.peer.isSuspectedDead {
		if n.peer.isSuspectedDead {
			log.Printf(" I think the peer is dead")
		}
		n.cluster.isLeader = true
		n.cluster.leaderID = n.id
		log.Printf("I won the election and I am now the leader!")
	} else {
		n.cluster.isLeader = false
		n.cluster.leaderID = n.peer.id
		log.Printf("I lost the election and '%v' is now the leader!", n.peer.id)
	}
}

func (n *Node) Heartbeat(context.Context, *as.PingRequest) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}

func Serve() {
	if len(os.Args) < 5 {
		fmt.Printf("Incorrect arguments supplied. Usage: [id] [addr] [peerId] [peerAddr]\n")
		os.Exit(1)
	}

	id, eID := strconv.ParseInt(os.Args[1], 10, 64)
	addr := os.Args[2]
	peerId, ePID := strconv.ParseInt(os.Args[3], 10, 64)
	peerAddr := os.Args[4]

	if eID != nil || ePID != nil {
		fmt.Printf("Please use a number as id for the nodes!\n")
		os.Exit(1)
	}

	node := NewNode(id, addr, peerId, peerAddr)

	go node.startHeartbeatLoop(500*time.Millisecond, 2*time.Second)

	lis, err := net.Listen("tcp", node.addr)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	as.RegisterAuctionServiceServer(grpcServer, node)
	as.RegisterNodeServiceServer(grpcServer, node)

	log.Printf("Started listening to %v", addr)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
