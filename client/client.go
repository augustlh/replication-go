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
	conn []proto.AuctionServiceClient
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
		conn, _, err := NewClientConnection(node)
		if err != nil {
			continue
		}

		_, err = conn.Ping(context.Background(), &proto.Nothing{})

		if err != nil {
			continue
		}

		client.conn = append(client.conn, conn)
	}

	return client
}

func PrintHelp() {
	fmt.Printf("Make bid:\n")
	fmt.Printf("./client [username] [positive bid amount]\n")
	fmt.Printf("Check current result/status:\n")
	fmt.Printf("./client result\n")
}

func Result(client *Client) (*proto.Outcome, bool){
	for _, conn := range client.conn {
		outcome, err := conn.Result(context.Background(), &proto.Nothing{})

		if err != nil {
			continue
		}

		return outcome, true
	}

	fmt.Printf("All servers are down, cannot see results\n")
	return nil, false
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

	outcome, success := Result(client)

	if !success {
		fmt.Println("All nodes are dead")
		return
	}

	if outcome.IsFinal {
		fmt.Println("Auction is over, cannot bid")
		return
	}

	bid := proto.ClientBid{Username: username, Amount: uint32(bidAmount), Item: outcome.Bid.Item}
	for _, conn := range client.conn {
		conn.Bid(context.Background(), &bid)
	}
}

func main() {
	if len(os.Args) < 2 {
		PrintHelp()
		return
	}

	client := NewClient("./servers.txt")

	if os.Args[1] == "result" {
		outcome, success := Result(client)
		
		if !success {
			fmt.Printf("All nodes dead, cannot get result\n")
			return
		}

		if outcome.IsFinal {
			fmt.Printf("Final outcome: %s won with bid at %d for %s\n",
				outcome.Bid.Username, outcome.Bid.Amount, outcome.Bid.Item)
		} else {
			fmt.Printf("Ongoing auction: %s with bid at %d for %s\n",
				outcome.Bid.Username, outcome.Bid.Amount, outcome.Bid.Item)
		}
	} else {
		Bid(client)
	}


}

