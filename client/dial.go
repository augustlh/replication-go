package main

import (
	"fmt"
	"log"

	as "auction_system/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func dialAny(addrs []string) (*grpc.ClientConn, as.AuctionServiceClient, error) {
	if len(addrs) == 0 {
		return nil, nil, fmt.Errorf("no node addresses configured")
	}

	var lastErr error

	for _, addr := range addrs {
		log.Printf("Client: trying to connect to %s", addr)

		conn, err := grpc.NewClient(
			addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			log.Printf("Client: failed to create client for %s: %v", addr, err)
			lastErr = err
			continue
		}

		log.Printf("Client: created client connection to %s", addr)

		client := as.NewAuctionServiceClient(conn)
		return conn, client, nil
	}

	if lastErr == nil {
		lastErr = fmt.Errorf("failed to connect to any node")
	}
	return nil, nil, lastErr
}
