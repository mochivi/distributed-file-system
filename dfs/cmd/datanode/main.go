package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/mochivi/distributed-file-system/internal/coordinator"
	"github.com/mochivi/distributed-file-system/internal/datanode"
	"github.com/mochivi/distributed-file-system/internal/storage/chunk"
	"github.com/mochivi/distributed-file-system/pkg/proto"
	"google.golang.org/grpc"
)

func initServer() (*datanode.DataNodeServer, error) {
	// ChunkStore implementation is chosen here
	cwd, err := os.Getwd()
	if err != nil {
		return nil, fmt.Errorf("unable to get cwd: %v", err)
	}
	rootDir := filepath.Join(cwd, "chunks")
	chunkStore := chunk.NewChunkDiskStorage(rootDir)

	// Create default datanode config
	nodeConfig := datanode.DefaultDatanodeConfig()

	// Replication and session manager implementations
	nodeSelector := datanode.NewNodeSelector()
	replicationManager := datanode.NewReplicationManager(nodeConfig.Replication, nodeSelector)
	sessionManager := datanode.NewSessionManager()

	return datanode.NewDataNodeServer(chunkStore, replicationManager, sessionManager, nodeConfig), nil
}

func registerDataNode(config datanode.DataNodeConfig) {
	coordinatorClient, err := coordinator.NewCoordinatorClient(fmt.Sprintf("%s:%d", config.Coordinator.Host, config.Coordinator.Port))
	if err != nil {
		log.Fatalf("failed to create coordinator client: %v", err)
	}

	req := coordinator.RegisterDataNodeRequest{NodeInfo: config.Info}
	resp, err := coordinatorClient.RegisterDataNode(context.Background(), req)
	if err != nil {
		log.Fatalf("failed to register datanode with coordinator: %v", err)
	}

	if !resp.Success {
		log.Fatalf("failed to register datanode with coordinator: %s", resp.Message)
	}
}

func main() {
	// Datanode server
	server, err := initServer()
	if err != nil {
		log.Fatalf("failed to initialize datanode server: %v", err)
	}

	// gRPC initialization
	grpcServer := grpc.NewServer()
	proto.RegisterDataNodeServiceServer(grpcServer, server)

	// Setup listener for gRPC server
	listener, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", server.Config.Info.Port))
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("Recovered from panic in gRPC server: %v", r)
				grpcServer.GracefulStop()
			}
		}()

		log.Printf("Starting datanode gRPC server on :%d", server.Config.Info.Port)
		if err := grpcServer.Serve(listener); err != nil {
			log.Fatalf("Failed to server gRPC server: %v", err)
		}
	}()

	// Register datanode with coordinator - if fails, should crash
	registerDataNode(server.Config)

	// Graceful shutdown on SIGINT/SIGTERM
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
	<-ch

	log.Println("Stopping gRPC server...")
	grpcServer.GracefulStop()
	log.Println("Server stopped, exiting...")
}
