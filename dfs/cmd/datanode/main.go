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

	// Create default datanode config
	nodeConfig := datanode.DefaultDatanodeConfig()

	// Replication and session manager implementations
	nodeSelector := datanode.NewNodeSelector()
	replicationManager := datanode.NewReplicationManager(nodeConfig.Replication, nodeSelector)
	sessionManager := datanode.NewSessionManager()

	return datanode.NewDataNodeServer(chunkStore, replicationManager, sessionManager, nodeConfig), nil
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

	// Subscribe to node updates
	coordinatorClient, err := coordinator.NewCoordinatorClient(coordinatorAddress)
	if err != nil {
		log.Fatalf("Failed to create coordinator client: %v", err)
	}

	// Start node update subscription in a goroutine
	go func() {
		req := &proto.SubscribeToNodeUpdatesRequest{
			NodeId: server.Config.Info.ID,
		}
		stream, err := coordinatorClient.SubscribeToNodeUpdates(context.Background(), req)
		if err != nil {
			log.Printf("Failed to subscribe to node updates: %v", err)
			return
		}

		for {
			update, err := stream.Recv()
			if err != nil {
				log.Printf("Error receiving node update: %v", err)
				return
			}

			switch update.Type {
			case proto.NodeUpdate_NODE_ADDED, proto.NodeUpdate_NODE_UPDATED:
				nodeInfo := common.DataNodeInfoFromProto(update.Node)
				server.nodeSelector.UpdateNodes([]common.DataNodeInfo{nodeInfo})
			case proto.NodeUpdate_NODE_REMOVED:
				server.nodeSelector.RemoveNode(update.Node.Id)
			}
		}
	}()

	// Graceful shutdown on SIGINT/SIGTERM
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
	<-ch

	log.Println("Stopping gRPC server...")
	grpcServer.GracefulStop()
	log.Println("Server stopped, exiting...")
}
