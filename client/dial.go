// client/dial.go
package main

import (
	"context"
	"fmt"
	"time"

	as "auction_system/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
)

func dialAny(addrs []string) (*grpc.ClientConn, as.AuctionServiceClient, error) {
	if len(addrs) == 0 {
		return nil, nil, fmt.Errorf("no node addresses configured")
	}

	dialParams := grpc.ConnectParams{
		MinConnectTimeout: 500 * time.Millisecond,
	}

	var lastErr error

	for _, addr := range addrs {
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)

		conn, err := grpc.NewClient(
			addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithConnectParams(dialParams),
		)
		if err != nil {
			cancel()
			lastErr = err
			continue
		}

		state := conn.GetState()
		if state != connectivity.Ready {
			if !conn.WaitForStateChange(ctx, state) {

				cancel()
				conn.Close()
				lastErr = ctx.Err()
				continue
			}

			if conn.GetState() != connectivity.Ready {
				cancel()
				conn.Close()
				lastErr = fmt.Errorf("connection to %s did not reach READY", addr)
				continue
			}
		}

		cancel()

		client := as.NewAuctionServiceClient(conn)
		return conn, client, nil
	}

	if lastErr == nil {
		lastErr = fmt.Errorf("failed to connect to any node")
	}
	return nil, nil, lastErr
}
