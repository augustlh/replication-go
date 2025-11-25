package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"sync"
	"sync/atomic"

	pb "github.com/augustlh/replication-go/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type Bid struct {
	bidderId string
	bid      uint
}

type Peer struct {
	id             uint
	isPresumedDead bool
	conn           pb.AuctionServiceClient
}

type Node struct {
	pb.UnimplementedAuctionServiceServer

	id   uint
	ip   string
	peer *Peer

	nextRequestId atomic.Uint32
	nextLogIndex  atomic.Uint32

	currentLeader uint
	highestBid    *Bid

	mu   sync.Mutex
	cond *sync.Cond
}

func NewNode(id uint, ip string) *Node {
	n := &Node{
		id:            id,
		ip:            ip,
		peer:          nil,
		highestBid:    nil,
		currentLeader: 0,
	}

	n.cond = sync.NewCond(&n.mu)
	return n
}

func (n *Node) RunElection() {
	log.Printf("Running election")
	n.mu.Lock()
	defer n.mu.Unlock()

	_, err := n.peer.conn.CheckHealth(context.Background(), &pb.HealthRequest{})
	if err != nil {
		n.peer.isPresumedDead = true
	}

	if n.peer.id < n.id || n.peer.isPresumedDead {
		n.currentLeader = n.id
		log.Printf("An election was held, and I became the leader!")
	} else {
		n.currentLeader = n.peer.id
		log.Printf("I lost the election and '%d' is now the leader", n.peer.id)
	}
}

func (n *Node) DeferDelivery(logIndex uint) {
	for n.nextLogIndex.Load() != uint32(logIndex) {
		n.cond.Wait()
	}
}

func (n *Node) Bid(ctx context.Context, req *pb.BidRequest) (*pb.BidResponse, error) {
	if n.currentLeader != n.id {
		n.RunElection()
	}

	if n.currentLeader != n.id {
		log.Printf("Received bid request, but refused it because I'm not the leader")
		return &pb.BidResponse{Type: pb.MessageType_Unexpected, Message: "I am not the leader, please contact the leader"}, nil
	}

	logIndex := n.nextRequestId.Add(1) - 1
	log.Printf("Received bid an enqueued it, logIndex=%v", logIndex)

	n.mu.Lock()
	defer n.mu.Unlock()

	n.DeferDelivery(uint(logIndex))

	log.Printf("Now handling request with logIndex=%v", logIndex)

	replicationRequest := &pb.ReplicationRequest{LogIndex: logIndex, Bid: req}
	log.Printf("Trying to replicate")
	_, err := n.peer.conn.Replicate(context.Background(), replicationRequest)
	if err != nil {
		fmt.Printf("Replica has failed, will not replicate anymore")
		n.peer.isPresumedDead = true
	}
	log.Printf("Replication sucess")

	res := n.HandleBid(req)
	n.cond.Broadcast()
	n.nextLogIndex.Add(1)

	return res, nil

}

func (n *Node) HandleBid(req *pb.BidRequest) *pb.BidResponse {
	log.Printf("%v", n.highestBid)

	log.Printf("Validating bid %v", req)
	if n.highestBid != nil && uint32(n.highestBid.bid) >= req.Amount {
		log.Printf("Refused bid of '%d' from clientId '%s' with reason: bid too low\n", req.Amount, req.ClientId)
		return &pb.BidResponse{Type: pb.MessageType_Error, Message: "bid too low"}
	}

	log.Printf("Accepted bid %v", req)
	newBid := &Bid{
		bidderId: req.ClientId,
		bid:      uint(req.Amount),
	}
	n.highestBid = newBid

	return &pb.BidResponse{Type: pb.MessageType_Success, Message: ""}
}

func (n *Node) Replicate(ctx context.Context, req *pb.ReplicationRequest) (*pb.Ack, error) {
	// TODO: Right now I'm assuming that the leader can't get a replication request due to split brain
	// this is fair to assume due to the requirements of our exiercise

	n.mu.Lock()
	defer n.mu.Unlock()

	n.DeferDelivery(uint(req.LogIndex))

	n.HandleBid(req.Bid)
	n.cond.Broadcast()

	n.nextRequestId.Store(req.LogIndex + 1)
	n.nextLogIndex.Add(1)

	return &pb.Ack{}, nil

}

func (n *Node) Result(ctx context.Context, req *pb.ResultRequest) (*pb.ResultResponse, error) {
	var bid Bid

	n.mu.Lock()
	defer n.mu.Unlock()

	if n.highestBid == nil {
		bid = Bid{bidderId: "None", bid: 0}
	} else {
		bid = *n.highestBid
	}

	response := &pb.ResultResponse{
		HighestBidderId: bid.bidderId,
		HighestBid:      uint32(bid.bid),
		Finished:        false,
	}

	return response, nil
}

func (n *Node) Connect(ctx context.Context, req *pb.ConnectRequest) (*pb.Ack, error) {
	n.mu.Lock()

	if n.peer != nil {
		return nil, status.Errorf(codes.AlreadyExists, "I already have a replica, request denied.")
	}

	if req.Id < uint32(n.id) {
		return nil, status.Errorf(codes.Unauthenticated, "Dont attempt to connect to me if you are lower id!")
	}

	client, err := n.TryConnect(req.Ip)
	if err != nil {
		return nil, err
	}

	n.peer = &Peer{
		id:             uint(req.Id),
		conn:           client,
		isPresumedDead: false,
	}
	n.mu.Unlock()
	go n.RunElection()

	return &pb.Ack{}, nil

}

func (n *Node) TryConnect(ip string) (pb.AuctionServiceClient, error) {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	conn, err := grpc.NewClient(ip, opts...)
	if err != nil {
		log.Printf("fail to dial: %v, refusing connect request", err)
		return nil, status.Errorf(codes.FailedPrecondition, "Could not dial your provided ip, connect request refused.")
	}

	client := pb.NewAuctionServiceClient(conn)
	return client, nil

}

func (n *Node) CheckHealth(context.Context, *pb.HealthRequest) (*pb.Heartbeat, error) {
	return &pb.Heartbeat{}, nil
}

func main() {
	if len(os.Args) < 4 {
		fmt.Printf("Invalid argumens")
		os.Exit(0)
	}

	nodeId, _ := strconv.ParseUint(os.Args[1], 10, 64)
	ip := fmt.Sprintf("%s:%s", os.Args[2], os.Args[3])
	lis, err := net.Listen("tcp", ip)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
		return
	}
	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)

	node := NewNode(uint(nodeId), ip)

	pb.RegisterAuctionServiceServer(grpcServer, node)

	if len(os.Args) >= 7 {
		log.Printf("Trying to connect to other node")
		client, err := node.TryConnect(fmt.Sprintf("%s:%s", os.Args[4], os.Args[5]))

		if err != nil {
			log.Printf("Failed to establish connection with other node: %v", err)
			return
		}

		idtoconnecto, _ := strconv.ParseUint(os.Args[6], 10, 64)
		node.mu.Lock()
		_, err = client.Connect(context.Background(), &pb.ConnectRequest{Ip: ip, Id: uint32(nodeId)})
		if err != nil {
			fmt.Printf("Failed to establish connection with other node: %v", err)
		}

		node.peer = &Peer{
			id:             uint(idtoconnecto),
			conn:           client,
			isPresumedDead: false,
		}
		node.mu.Unlock()

		log.Printf("Sucessfully stablished connection")

		go node.RunElection()
	}

	log.Printf("now listening on: %s\n", ip)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("server exited: %v", err)
	}

	select {}
}
