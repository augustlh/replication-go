package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	cfg "auction_system/config"
	auction_system "auction_system/proto"

	"google.golang.org/protobuf/types/known/emptypb"
)

const defaultAddr = "localhost:50051"

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(1)
	}

	cmd := os.Args[1]

	conn, client, err := dialAny(nodeAddrs())
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer conn.Close()

	switch cmd {
	case "register":
		if len(os.Args) != 3 {
			usage()
			os.Exit(1)
		}
		doRegister(client, os.Args[2])

	case "bid":
		if len(os.Args) != 4 {
			usage()
			os.Exit(1)
		}
		amount, err := strconv.ParseInt(os.Args[3], 10, 64)
		if err != nil {
			log.Fatalf("invalid amount %q: %v", os.Args[3], err)
		}
		doBid(client, os.Args[2], amount)

	case "status":
		doStatus(client)

	default:
		usage()
		os.Exit(1)
	}
}

func usage() {
	fmt.Println("usage:")
	fmt.Println("  client register <bidder_id>")
	fmt.Println("  client bid <bidder_id> <amount>")
	fmt.Println("  client status")
	fmt.Println()
	fmt.Printf("nodes: AUCTION_NODES=\"host1:port1,host2:port2\" or AUCTION_ADDR (default %s)\n", defaultAddr)
}

func nodeAddrs() []string {
	addrs := make([]string, 0, len(cfg.Nodes))
	for _, n := range cfg.Nodes {
		addrs = append(addrs, n.Addr)
	}
	return addrs
}

func contextWithTimeout() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 2*time.Second)
}

func doRegister(client auction_system.AuctionServiceClient, bidderID string) {
	ctx, cancel := contextWithTimeout()
	defer cancel()

	resp, err := client.RegisterBidder(ctx, &auction_system.RegisterBidderReq{
		BidderId: bidderID,
	})
	if err != nil {
		log.Fatalf("RegisterBidder: %v", err)
	}

	fmt.Println("register result:")
	fmt.Printf("  ok:      %v\n", resp.Ok)
	fmt.Printf("  message: %s\n", resp.Message)
}

func doBid(client auction_system.AuctionServiceClient, bidderID string, amount int64) {
	ctx, cancel := contextWithTimeout()
	defer cancel()

	resp, err := client.PlaceBid(ctx, &auction_system.PlaceBidReq{
		BidderId: bidderID,
		Amount:   amount,
	})
	if err != nil {
		log.Fatalf("PlaceBid: %v", err)
	}

	fmt.Println("bid result:")
	fmt.Printf("  accepted: %v\n", resp.Accepted)
	fmt.Printf("  reason:   %s\n", resp.Reason)
}

func doStatus(client auction_system.AuctionServiceClient) {
	ctx, cancel := contextWithTimeout()
	defer cancel()

	resp, err := client.GetStatus(ctx, &emptypb.Empty{})
	if err != nil {
		log.Fatalf("GetStatus: %v", err)
	}

	fmt.Println("auction status:")
	fmt.Printf("  closed:         %v\n", resp.Closed)
	fmt.Printf("  highest_bid:    %d\n", resp.HighestBid)
	fmt.Printf("  highest_bidder: %s\n", resp.HighestBidder)
	fmt.Printf("  deadline:       %s\n", resp.Deadline.AsTime().Format(time.RFC3339))

	if len(resp.RegisteredBidders) > 0 {
		fmt.Println("  registered bidders:")
		for _, b := range resp.RegisteredBidders {
			fmt.Printf("    - %s\n", b)
		}
	}
}
