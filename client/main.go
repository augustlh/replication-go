package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	auction_system "auction_system/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

const defaultAddr = "localhost:5001"

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(1)
	}

	cmd := os.Args[1]

	addr := defaultAddr
	if envAddr := os.Getenv("AUCTION_ADDR"); envAddr != "" {
		addr = envAddr
	}

	conn, err := grpc.NewClient(
		addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatalf("failed to connect to %s: %v", addr, err)
	}
	defer conn.Close()

	client := auction_system.NewAuctionServiceClient(conn)

	switch cmd {
	case "register":
		if len(os.Args) != 3 {
			fmt.Println("usage: client register <bidder_id>")
			os.Exit(1)
		}
		bidderID := os.Args[2]
		doRegister(client, bidderID)

	case "bid":
		if len(os.Args) != 4 {
			fmt.Println("usage: client bid <bidder_id> <amount>")
			os.Exit(1)
		}
		bidderID := os.Args[2]
		amountStr := os.Args[3]

		amount, err := strconv.ParseInt(amountStr, 10, 64)
		if err != nil {
			log.Fatalf("invalid amount %q: %v", amountStr, err)
		}
		doBid(client, bidderID, amount)

	case "status":
		doStatus(client)

	default:
		fmt.Printf("unknown command %q\n", cmd)
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
	fmt.Printf("server address can be overridden with AUCTION_ADDR (default %s)\n", defaultAddr)
}

func contextWithTimeout() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 2*time.Second)
}

func doRegister(client auction_system.AuctionServiceClient, bidderID string) {
	ctx, cancel := contextWithTimeout()
	defer cancel()

	req := &auction_system.RegisterBidderReq{
		BidderId: bidderID,
	}

	resp, err := client.RegisterBidder(ctx, req)
	if err != nil {
		log.Fatalf("RegisterBidder RPC failed: %v", err)
	}

	fmt.Println("register result:")
	fmt.Printf("  ok:      %v\n", resp.Ok)
	fmt.Printf("  message: %s\n", resp.Message)
}

func doBid(client auction_system.AuctionServiceClient, bidderID string, amount int64) {
	ctx, cancel := contextWithTimeout()
	defer cancel()

	req := &auction_system.PlaceBidReq{
		BidderId: bidderID,
		Amount:   amount,
	}

	resp, err := client.PlaceBid(ctx, req)
	if err != nil {
		log.Fatalf("PlaceBid RPC failed: %v", err)
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
		log.Fatalf("GetStatus RPC failed: %v", err)
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
