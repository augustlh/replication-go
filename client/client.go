package client

import (
	"fmt"
	"net"
	"os"
	"log"
	"context"
	"sync/atomic"
	atm "replication-go/generic_atomic"
	proto "replication-go/grpc"
	"google.golang.org/grpc"
	_ "google.golang.org/grpc/credentials/insecure"
)

type Client struct {
	conn proto.AuctionServiceClient
}

func NewClient() *Client {

}

