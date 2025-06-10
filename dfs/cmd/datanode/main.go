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
	"github.com/mochivi/distributed-file-system/internal/datanode"
	"github.com/mochivi/distributed-file-system/internal/storage/chunk"
	"github.com/mochivi/distributed-file-system/pkg/proto"
	"github.com/mochivi/distributed-file-system/pkg/utils"
	"google.golang.org/grpc"
)

func initServer() (*datanode.DataNodeServer, error) {
	// ChunkStore implementation is chosen here
	baseDir := utils.GetEnvString("DISK_STORAGE_BASE_DIR", "/app")
	rootDir := filepath.Join(baseDir, "data")
	chunkStore, err := chunk.NewChunkDiskStorage(chunk.DiskStorageConfig{
		Enabled: true,
		Kind:    "block",
		RootDir: rootDir,
	})
	if err != nil {
		log.Fatal(err.Error())
	}

	// Datanode dependencies
	nodeConfig := datanode.DefaultDatanodeConfig()
	nodeSelector := common.NewNodeSelector()
	nodeManager := common.NewNodeManager(nodeSelector)
	replicationManager := datanode.NewReplicationManager(nodeConfig.Replication, nodeSelector)
	sessionManager := datanode.NewSessionManager()

	return datanode.NewDataNodeServer(chunkStore, replicationManager, sessionManager, nodeManager, nodeConfig), nil
}

func main() {
	if err := godotenv.Load(); err != nil {
		log.Fatalf("failed to load environment: %v", err)
	}

	// Datanode server
	server, err := initServer()
	if err != nil {
		log.Fatalf("failed to initialize datanode server: %v", err)
	}

	// gRPC initialization
	grpcServer := grpc.NewServer()
	proto.RegisterDataNodeServiceServer(grpcServer, server)

	// Setup listener for gRPC server
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", server.Config.Info.Port))
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
	coordinatorHost := utils.GetEnvString("COORDINATOR_HOST", "coordinator")
	coordinatorPort := utils.GetEnvInt("COORDINATOR_PORT", 8080)
	coordinatorAddress := fmt.Sprintf("%s:%d", coordinatorHost, coordinatorPort)

	// Temporarily, overwrite the datanode IP address with the docker dns name
	server.Config.Info.IPAddress = "datanode"

	// Register datanode with coordinator
	if err := server.RegisterWithCoordinator(context.TODO(), coordinatorAddress); err != nil {
		log.Fatalf("Failed to register with coordinator: %v", err)
	}

	// Graceful shutdown on SIGINT/SIGTERM
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
	<-ch

	log.Println("Stopping gRPC server...")
	grpcServer.GracefulStop()
	log.Println("Server stopped, exiting...")
}
