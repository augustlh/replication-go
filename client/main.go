package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	pb "github.com/augustlh/replication-go/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func EstablishConnection(ip string) pb.AuctionServiceClient {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))

	conn, err := grpc.NewClient(ip, opts...)
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}

	client := pb.NewAuctionServiceClient(conn)

	return client
}

type NodeInfo struct {
	ip     string
	client pb.AuctionServiceClient
	alive  bool
}

func PickNewLeader(nodes map[string]*NodeInfo) (*NodeInfo, bool) {
	for _, node := range nodes {
		if node.alive {
			return node, true
		}
	}
	return nil, false
}

func ChangeLeader(nodes map[string]*NodeInfo, currentLeader **NodeInfo) {
	for _, node := range nodes {
		if node.alive && node != *currentLeader {
			*currentLeader = node
			log.Printf("Leader changed to %s", node.ip)
			return
		}
	}
	log.Fatalf("Wasn't able to change leader. Both nodes have died")
}

func main() {
	if len(os.Args) < 4 {
		fmt.Printf("Usage: %s [node1_address] [node2_address] client_name]\n", os.Args[0])
		return
	}

	addresses := os.Args[1 : len(os.Args)-1]

	nodes := make(map[string]*NodeInfo)
	for _, address := range addresses {
		nodes[address] = &NodeInfo{ip: address, alive: true, client: EstablishConnection(address)}
	}

	currentLeader, ok := PickNewLeader(nodes)
	if !ok {
		log.Fatal("Both of the nodes you've entered has died")
	}

	name := os.Args[len(os.Args)-1]

	reader := bufio.NewReader(os.Stdin)
	fmt.Printf("Welcome %s! Please enter a command, use 'help' to get started.\n", name)
	fmt.Print("> ")

	for {
		text, _ := reader.ReadString('\n')
		text = strings.TrimSpace(text)
		if text == "" {
			continue
		}

		parts := strings.Fields(text)
		command := strings.ToLower(parts[0])

		switch command {
		case "bid":

			if len(parts) < 2 {
				fmt.Println("Please specify an amount: bid [amount]")
				break
			}

			amount, err := strconv.Atoi(parts[1])
			if err != nil || amount <= 0 {
				break
			}

			req := &pb.BidRequest{
				ClientId: name,
				Amount:   uint32(amount),
			}
			res, err := currentLeader.client.Bid(context.Background(), req)
			if err != nil {
				fmt.Printf("Please try again.\n")

				currentLeader.alive = false
				ChangeLeader(nodes, &currentLeader)

			} else {
				switch res.Type {
				case pb.MessageType_Error:
					fmt.Printf("Your bid was declined with the reason '%v'\n", res.Message)
				case pb.MessageType_Success:
					fmt.Printf("Your bid of '%v' was accepted\n", req.Amount)
				case pb.MessageType_Unexpected:
					fmt.Printf("Please try again.\n")
					ChangeLeader(nodes, &currentLeader)
				}
			}

		case "status":
			res, err := currentLeader.client.Result(context.Background(), &pb.ResultRequest{})
			if err != nil {
				fmt.Println("Failed to retrieve auction status, please try again!")
				ChangeLeader(nodes, &currentLeader)
				break
			}

			fmt.Printf("Status of the auction is: \n%v\n", res)
		default:
			fmt.Printf("Please use either:\n\tbid [amount]\t\t tries to place a bid\n\tstatus\t\tshows the current status of the auction\n\thelp\t\tdisplays this message\n")
		}

		fmt.Print("> ")
	}
}
