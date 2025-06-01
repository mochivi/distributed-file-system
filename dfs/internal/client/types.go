package client

import (
	"github.com/mochivi/distributed-file-system/pkg/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Implements proto.DataNodeServiceClient
type DataNodeClient struct {
	client proto.DataNodeServiceClient
	conn   *grpc.ClientConn
}

func NewDataNodeClient(serverAddress string) (*DataNodeClient, error) {
	conn, err := grpc.NewClient(
		serverAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()), // Update to TLS in prod
	)
	if err != nil {
		return nil, err
	}

	client := proto.NewDataNodeServiceClient(conn)

	return &DataNodeClient{
		client: client,
		conn:   conn,
	}, nil
}
