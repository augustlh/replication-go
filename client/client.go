package main

import (
	"fmt"
	"log"
	"context"
	"replication-go/common"
	proto "replication-go/grpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Client struct {
	conn proto.AuctionServiceClient
}

func NewClientConnection(node common.NodeInfo) (proto.AuctionServiceClient, error) {
	conn, err := grpc.NewClient(node.ConnectionAddr, 
		grpc.WithTransportCredentials(insecure.NewCredentials()));

	if err != nil {
		log.Printf("Error connecting: %s", err.Error())
		return nil, err
	}
	return proto.NewAuctionServiceClient(conn), nil
}

func NewClient(node common.NodeInfo) *Client {
	client := new(Client)

	conn, err := NewClientConnection(node)
	if err != nil {
		panic(fmt.Sprintf("Could not connect to node: %s", node.ConnectionAddr))
	}

	client.conn = conn

	leaderInfo, err := client.conn.WhoIsTheLeader(context.Background(), &proto.Nothing{})

	if err != nil {
		panic("Could not determine leader node")
	}

	if leaderInfo.ConnectionAddr != node.ConnectionAddr {
		newConn, err := NewClientConnection(common.NodeInfo { 
			ConnectionAddr: leaderInfo.ConnectionAddr })

		if err != nil {
			panic("Could not connect to leader node")
		}
		client.conn = newConn
	}

	return client
}

func (client *Client) MakeBid() {
	bid := proto.ClientBid{Username: "example", Amount: 55, Item: "Something"}
	client.conn.Bid(context.Background(), &bid)
}

func main() {
	info := common.NodeInfo { ConnectionAddr: "localhost:5000" }

	client := NewClient(info)

	client.MakeBid()

}

