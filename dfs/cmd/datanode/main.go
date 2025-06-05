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

	"github.com/joho/godotenv"
	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/internal/coordinator"
	"github.com/mochivi/distributed-file-system/internal/datanode"
	"github.com/mochivi/distributed-file-system/internal/storage/chunk"
	"github.com/mochivi/distributed-file-system/pkg/proto"
	"github.com/mochivi/distributed-file-system/pkg/utils"
	"google.golang.org/grpc"
)

func initServer() *datanode.DataNodeServer {
	// ChunkStore implementation is chosen here -- configure to choose from some environment variable
	cwd, err := os.Getwd()
	if err != nil {
		log.Fatalf("unable to get cwd: %v", err)
	}
	rootDir := filepath.Join(cwd, "chunks")
	chunkStore := chunk.NewChunkDiskStorage(rootDir)

	// Configure how the data node replicates its chunks
	replicationConfig := datanode.ReplicateManagerConfig{
		ReplicateTimeout: 1,  // minutes
		ChunkStreamSize:  64, // kB
		MaxChunkRetries:  3,
	}
	nodeSelector := datanode.NewNodeSelector()
	replicationManager := datanode.NewReplicationManager(replicationConfig, nodeSelector)

	// SessionManager implementation
	sessionManager := datanode.NewSessionManager()

	// Gather information about this data node
	info := gatherDataNodeInfo()

	// Configure how the datanode operates
	nodeConfig := datanode.DataNodeConfig{
		SessionManagerConfig: datanode.SessionManagerConfig{
			SessionTimeout: 15, // minutes
		},
	}

	return datanode.NewDataNodeServer(chunkStore, replicationManager, sessionManager, info, nodeConfig)
}

// Initial implementation is completely based on environment variables being setup
// TODO: there should be a step reading from a datanode-config.yml file or similar to setup the datanode
func gatherDataNodeInfo() common.DataNodeInfo {

	datanodeIPAddress := utils.GetEnvString("DATANODE_IP_ADDRESS", "127.0.0.1")
	datanodePort := utils.GetEnvInt("DATANODE_PORT", 8081)
	datanodeCapacity := utils.GetEnvInt("DATANODE_CAPACITY_GB", 10) // Default to 10 GB reserved for storage only

	return common.DataNodeInfo{
		ID:        "TBD", // should be unique somehow
		IPAddress: datanodeIPAddress,
		Port:      datanodePort,
		Capacity:  datanodeCapacity * 1024 * 1024 * 1024, // Must be at most how much is available in the system
		Used:      0,                                     // Should be recalculated if the node has been active before (perhaps the node will store its own information in disk too)
		Status:    common.NodeHealthy,
	}
}

func registerDataNode(datanodeInfo common.DataNodeInfo) {
	coordinatorAddress := utils.GetEnvString("COORDINATOR_ADDRESS", "localhost")
	coordinatorClient, err := coordinator.NewCoordinatorClient(coordinatorAddress)
	if err != nil {
		log.Fatalf("failed to create coordinator client: %v", err)
	}

	req := coordinator.RegisterDataNodeRequest{NodeInfo: datanodeInfo}
	resp, err := coordinatorClient.RegisterDataNode(context.Background(), req)
	if err != nil {
		log.Fatalf("failed to register datanode with coordinator: %v", err)
	}

	if !resp.Success {
		log.Fatalf("failed to register datanode with coordinator: %s", resp.Message)
	}
}

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	grpcServer := grpc.NewServer()

	server := initServer()

	proto.RegisterDataNodeServiceServer(grpcServer, server)

	listener, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", server.Info.Port))
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

		log.Printf("Starting gRPC server on :%d", server.Info.Port)
		if err := grpcServer.Serve(listener); err != nil {
			log.Fatalf("Failed to server gRPC server: %v", err)
		}
	}()

	// Register datanode with coordinator - if fails, should crash
	registerDataNode(server.Info)

	// Graceful shutdown on SIGINT/SIGTERM
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
	<-ch

	log.Println("Stopping gRPC server...")
	grpcServer.GracefulStop()
	log.Println("Server stopped, exiting...")
}
