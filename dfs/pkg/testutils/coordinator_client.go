package testutils

import (
	"crypto/rand"
	"encoding/hex"
	"net"
	"strconv"
	"testing"

	"github.com/mochivi/distributed-file-system/internal/clients"
	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/pkg/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// NewTestCoordinatorClientWithStubServer starts a stubCoordinatorServer listening on a free TCP port and
// returns the coordinator client and a cleanup function.
func NewTestCoordinatorClientWithStubServer(t *testing.T, server proto.CoordinatorServiceServer) (*clients.CoordinatorClient, func()) {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	proto.RegisterCoordinatorServiceServer(grpcServer, server)

	go func() {
		_ = grpcServer.Serve(lis)
	}()

	nodeInfo := &common.DataNodeInfo{
		ID:   "node-1",
		Host: "127.0.0.1",
		Port: func() int {
			host, port, _ := net.SplitHostPort(lis.Addr().String())
			_ = host // already 127.0.0.1
			p, _ := strconv.Atoi(port)
			return p
		}(),
		Status: common.NodeHealthy,
	}

	coordinatorClient, err := clients.NewCoordinatorClient(nodeInfo, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to create coordinator client: %v", err)
	}

	return coordinatorClient, func() {
		coordinatorClient.Close()
		grpcServer.GracefulStop()
		_ = lis.Close()
	}
}

// NewTestDataNodeClientWithStubServer starts a stubDataNodeServer listening on a free TCP port and
// returns the data node client and a cleanup function.
func NewTestDataNodeClientWithStubServer(t *testing.T, server proto.DataNodeServiceServer) (*clients.DataNodeClient, func()) {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	proto.RegisterDataNodeServiceServer(grpcServer, server)

	go func() {
		_ = grpcServer.Serve(lis)
	}()

	nodeInfo := &common.DataNodeInfo{
		ID:   "node-1",
		Host: "127.0.0.1",
		Port: func() int {
			host, port, _ := net.SplitHostPort(lis.Addr().String())
			_ = host // already 127.0.0.1
			p, _ := strconv.Atoi(port)
			return p
		}(),
		Status: common.NodeHealthy,
	}

	dataNodeClient, err := clients.NewDataNodeClient(nodeInfo)
	if err != nil {
		t.Fatalf("failed to create data node client: %v", err)
	}

	return dataNodeClient, func() {
		dataNodeClient.Close()
		grpcServer.GracefulStop()
		_ = lis.Close()
	}
}

// Helper functions for testing

// newRandomChunk creates chunk header + data for unit testing.
func NewRandomChunk(size int) (common.ChunkHeader, []byte) {
	data := make([]byte, size)
	_, _ = rand.Read(data)
	checksum := common.CalculateChecksum(data)

	header := common.ChunkHeader{
		ID:       "chunk-" + hex.EncodeToString(data[:4]),
		Version:  1,
		Index:    0,
		Size:     int64(len(data)),
		Checksum: checksum,
	}
	return header, data
}
