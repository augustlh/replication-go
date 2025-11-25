package server

import (
	cfg "auction_system/config"
	as "auction_system/proto"

	"context"
	"flag"
	"log"
	"net"
	"strconv"
	"strings"
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
	id            int64
	nodeClient    as.NodeServiceClient
	auctionClient as.AuctionServiceClient
}

type Node struct {
	as.UnimplementedAuctionServiceServer
	as.UnimplementedNodeServiceServer

	id   int64
	addr string

	mu      sync.RWMutex
	auction *AuctionState
	cluster ClusterState
	peers   map[int64]*Peer
}

func parsePeers(raw string) map[int64]string {
	m := make(map[int64]string)
	if raw == "" {
		return m
	}
	entries := strings.Split(raw, ",")
	for _, e := range entries {
		parts := strings.SplitN(strings.TrimSpace(e), "=", 2)
		if len(parts) != 2 {
			continue
		}
		id, err := strconv.ParseInt(parts[0], 10, 64)
		if err != nil {
			continue
		}
		m[id] = parts[1]
	}
	return m
}

func NewNode(id int64, addr string, peers map[int64]string) *Node {
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
		peers:   make(map[int64]*Peer),
	}

	leaderID := id
	for pid := range peers {
		if pid > leaderID {
			leaderID = pid
		}
	}
	n.cluster.leaderID = leaderID
	n.cluster.isLeader = id == leaderID

	for pid, paddr := range peers {
		if pid == id {
			continue
		}

		conn, err := grpc.NewClient(
			paddr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			panic(err)
		}

		n.peers[pid] = &Peer{
			id:            pid,
			auctionClient: as.NewAuctionServiceClient(conn),
			nodeClient:    as.NewNodeServiceClient(conn),
		}
	}

	return n
}

func NewNodeFromConfig(id int64) *Node {
	var selfAddr string
	peers := make(map[int64]string)

	for _, n := range cfg.Nodes {
		if n.ID == id {
			selfAddr = n.Addr
		} else {
			peers[n.ID] = n.Addr
		}
	}

	if selfAddr == "" {
		log.Fatalf("node id %d not found in config.Nodes", id)
	}

	return NewNode(id, selfAddr, peers)
}

func (n *Node) toProtoState() *as.AuctionState {
	return &as.AuctionState{
		AuctionId:         0,
		HighestBid:        n.auction.HighestBid,
		HighestBidder:     n.auction.HighestBidder,
		Closed:            n.auction.Closed,
		DeadlineUnix:      n.auction.Deadline.Unix(),
		RegisteredBidders: append([]string(nil), n.auction.RegisteredBidders...),
	}
}

func (n *Node) applyProtoState(st *as.AuctionState) {
	n.auction.HighestBid = st.HighestBid
	n.auction.HighestBidder = st.HighestBidder
	n.auction.Closed = st.Closed
	n.auction.Deadline = time.Unix(st.DeadlineUnix, 0)
	n.auction.RegisteredBidders = append([]string(nil), st.RegisteredBidders...)
}

func (n *Node) replicateState(ctx context.Context) {
	n.mu.RLock()
	st := n.toProtoState()
	peers := make([]*Peer, 0, len(n.peers))
	for _, p := range n.peers {
		peers = append(peers, p)
	}
	n.mu.RUnlock()

	for _, peer := range peers {
		_, _ = peer.nodeClient.ReplicateState(ctx, st)
	}
}

func (n *Node) ReplicateState(ctx context.Context, st *as.AuctionState) (*emptypb.Empty, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.applyProtoState(st)
	return &emptypb.Empty{}, nil
}

func (n *Node) PlaceBid(ctx context.Context, req *as.PlaceBidReq) (*as.PlaceBidResp, error) {
	n.mu.RLock()
	isLeader := n.cluster.isLeader
	leaderID := n.cluster.leaderID
	leaderPeer := n.peers[leaderID]
	n.mu.RUnlock()

	if !isLeader {
		if leaderPeer == nil {
			return &as.PlaceBidResp{Accepted: false, Reason: "no leader available"}, nil
		}
		return leaderPeer.auctionClient.PlaceBid(ctx, req)
	}

	n.mu.Lock()
	now := time.Now()

	if n.auction.Closed || now.After(n.auction.Deadline) {
		n.auction.Closed = true
		n.mu.Unlock()
		n.replicateState(ctx)
		return &as.PlaceBidResp{Accepted: false, Reason: "auction closed"}, nil
	}

	found := false
	for _, b := range n.auction.RegisteredBidders {
		if b == req.BidderId {
			found = true
			break
		}
	}
	if !found {
		n.auction.RegisteredBidders = append(n.auction.RegisteredBidders, req.BidderId)
	}

	if req.Amount <= n.auction.HighestBid {
		n.mu.Unlock()
		return &as.PlaceBidResp{Accepted: false, Reason: "bid too low"}, nil
	}

	n.auction.HighestBid = req.Amount
	n.auction.HighestBidder = req.BidderId
	n.mu.Unlock()

	n.replicateState(ctx)

	return &as.PlaceBidResp{Accepted: true, Reason: ""}, nil
}

func (n *Node) GetStatus(ctx context.Context, _ *emptypb.Empty) (*as.GetStatusResp, error) {
	n.mu.RLock()
	isLeader := n.cluster.isLeader
	leaderID := n.cluster.leaderID
	leaderPeer := n.peers[leaderID]
	n.mu.RUnlock()

	if !isLeader {
		if leaderPeer == nil {
			return &as.GetStatusResp{Closed: true}, nil
		}
		return leaderPeer.auctionClient.GetStatus(ctx, &emptypb.Empty{})
	}

	n.mu.RLock()
	defer n.mu.RUnlock()

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
		leaderID := n.cluster.leaderID
		peer := n.peers[leaderID]
		n.mu.RUnlock()

		if isLeader {
			continue
		}

		if peer == nil {
			n.handleLeaderSuspected()
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), heartbeatTimeout)
		_, err := peer.nodeClient.Heartbeat(ctx, &as.PingRequest{FromId: n.id})
		cancel()

		if err != nil {
			n.handleLeaderSuspected()
		}
	}
}

func (n *Node) handleLeaderSuspected() {
	n.mu.Lock()
	defer n.mu.Unlock()

	delete(n.peers, n.cluster.leaderID)

	for _, peer := range n.peers {
		if peer.id > n.id {
			return
		}
	}

	const electionTimeout = 2 * time.Second

	for _, peer := range n.peers {
		ctx, cancel := context.WithTimeout(context.Background(), electionTimeout)
		resp, err := peer.nodeClient.Election(ctx, &as.ElectionRequest{
			CandidateId: n.id,
		})
		cancel()

		if err != nil {
			continue
		}

		if !resp.Accepted {
			return
		}
	}

	n.cluster.leaderID = n.id
	n.cluster.isLeader = true

	const announceTimeout = 2 * time.Second

	for _, peer := range n.peers {
		ctx, cancel := context.WithTimeout(context.Background(), announceTimeout)
		_, _ = peer.nodeClient.AnnounceLeader(ctx, &as.LeaderAnnouncement{
			LeaderId: n.id,
		})
		cancel()
	}
}

func (n *Node) Heartbeat(context.Context, *as.PingRequest) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}

func (n *Node) Election(_ context.Context, req *as.ElectionRequest) (*as.ElectionResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	accepted := req.CandidateId > n.id
	return &as.ElectionResponse{Accepted: accepted, ResponderId: n.id}, nil
}

func (n *Node) AnnounceLeader(_ context.Context, req *as.LeaderAnnouncement) (*emptypb.Empty, error) {
	n.mu.Lock()
	n.cluster.leaderID = req.LeaderId
	n.cluster.isLeader = n.id == req.LeaderId
	n.mu.Unlock()
	return &emptypb.Empty{}, nil
}

func Serve() {
	idFlag := flag.Int("id", 0, "node id")
	flag.Parse()

	if *idFlag == 0 {
		log.Fatalf("--id must be provided")
	}
	id := int64(*idFlag)

	node := NewNodeFromConfig(id)

	go node.startHeartbeatLoop(500*time.Millisecond, 2*time.Second)

	lis, err := net.Listen("tcp", node.addr)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	as.RegisterAuctionServiceServer(grpcServer, node)
	as.RegisterNodeServiceServer(grpcServer, node)

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
