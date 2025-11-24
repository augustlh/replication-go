package main

import (
	"os"
	"fmt"
	"log"
	"strconv"
	"context"
	"replication-go/common"
	proto "replication-go/grpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Client struct {
	conn proto.AuctionServiceClient
}

func NewClientConnection(node common.NodeInfo) (proto.AuctionServiceClient, *grpc.ClientConn, error) {
	conn, err := grpc.NewClient(node.ConnectionAddr, 
		grpc.WithTransportCredentials(insecure.NewCredentials()));

	if err != nil {
		log.Printf("Error connecting: %s", err.Error())
		return nil, nil, err
	}
	return proto.NewAuctionServiceClient(conn), conn, nil
}

func NewClient(file string) *Client {
	client := new(Client)

	nodes, err := common.ReadServerFile(file)
	if err != nil {
		panic(fmt.Sprintf("Could not load servers from %s - %s", file, err.Error()))
	}

	for _, node := range nodes {
		conn, tcpConn, err := NewClientConnection(node)
		if err != nil {
			continue
		}

		leaderInfo, err := conn.WhoIsTheLeader(context.Background(), &proto.Nothing{})

		if err != nil {
			continue
			panic("Could not determine leader node")
		}

		if node.ConnectionAddr != leaderInfo.ConnectionAddr {
			tcpConn.Close()
			conn, _, err := NewClientConnection( 
				common.NodeInfo {ConnectionAddr: leaderInfo.ConnectionAddr},
				)

			if err != nil { panic(err.Error()) }

			client.conn = conn
		} else {
			client.conn = conn
		}
	}

	return client
}

func (client *Client) MakeBid() {
	bid := proto.ClientBid{Username: "example", Amount: 55, Item: "Something"}
	client.conn.Bid(context.Background(), &bid)
}

func PrintHelp() {
	fmt.Printf("Make bid:\n")
	fmt.Printf("./client [username] [positive bid amount]\n")
	fmt.Printf("Check current result/status:\n")
	fmt.Printf("./client result\n")
}

func Result(client *Client) {
}

func Bid(client *Client) {
	if len(os.Args) < 3 {
		PrintHelp()
		return
	}

	bidAmount, err := strconv.Atoi(os.Args[2])
	if err != nil || bidAmount <= 0 {
		PrintHelp()
		return
	}

	username := os.Args[1]

	bid := proto.ClientBid{Username: username, Amount: uint32(bidAmount), Item: "Something"}
	client.conn.Bid(context.Background(), &bid)
}

func main() {
	if len(os.Args) < 2 {
		PrintHelp()
		return
	}

	client := NewClient("./servers.txt")

	if os.Args[1] == "result" {
		Result(client)
	} else {
		Bid(client)
	}


}

