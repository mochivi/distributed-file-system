package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/mochivi/distributed-file-system/internal/cluster"
	datanode_controllers "github.com/mochivi/distributed-file-system/internal/cluster/datanode/controllers"
	datanode_services "github.com/mochivi/distributed-file-system/internal/cluster/datanode/services"
	"github.com/mochivi/distributed-file-system/internal/cluster/state"
	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/internal/config"
	"github.com/mochivi/distributed-file-system/internal/datanode"
	"github.com/mochivi/distributed-file-system/internal/storage/chunk"
	"github.com/mochivi/distributed-file-system/internal/storage/encoding"
	"github.com/mochivi/distributed-file-system/pkg/logging"
	"github.com/mochivi/distributed-file-system/pkg/proto"
	"github.com/mochivi/distributed-file-system/pkg/streamer"
	"github.com/mochivi/distributed-file-system/pkg/utils"
	"google.golang.org/grpc"
)

// COORDINATED SHUTDOWN PROCESS:
// This section implements a graceful shutdown pattern for multiple goroutines
// with both context-aware and non-context-aware services.
//
// STEP 1: Wait for shutdown trigger (signal or error)
// - Block on either OS signal (SIGINT/SIGTERM) or error from any goroutine
// - This is the single point that initiates shutdown
//
// STEP 2: Cancel context to stop context-aware services
// - Signals heartbeat loop (and any other context-aware goroutines) to stop
// - gRPC server is NOT context-aware, so this won't stop it directly
//
// STEP 3: Initiate gRPC server graceful shutdown
// - Start GracefulStop() in separate goroutine to avoid blocking
// - This will cause grpcServer.Serve() to return, allowing gRPC goroutine to exit
//
// STEP 4: Wait for all goroutines to finish
// - wg.Wait() blocks until both heartbeat and gRPC goroutines call wg.Done()
// - Heartbeat goroutine exits when it detects context cancellation
// - gRPC goroutine exits when Serve() returns due to GracefulStop()
// - Timeout protection prevents hanging if goroutines don't exit cleanly
//
// STEP 5: Ensure gRPC server shutdown completion
// - Additional timeout protection for GracefulStop() operation
// - Falls back to forced Stop() if graceful shutdown takes too long
//
// WHY THIS WORKS:
// - Context cancellation and GracefulStop() run concurrently
// - Both operations are needed because services have different shutdown mechanisms
// - WaitGroup ensures we don't exit until all cleanup is complete
// - Multiple timeout layers prevent process from hanging indefinitely
// - Buffered error channel prevents goroutines from blocking during shutdown
func main() {
	// Load configuration
	appConfig, err := config.LoadDatanodeConfig(".") // Load datanode-specific config
	if err != nil {
		log.Fatalf("failed to load configuration: %v", err)
	}

	// Load datanode info
	datanodeHost := utils.GetEnvString("DATANODE_HOST", "0.0.0.0")
	datanodePort := utils.GetEnvInt("DATANODE_PORT", 8081)
	datanodeInfo := common.NodeInfo{
		ID:       uuid.NewString(),
		Host:     datanodeHost,
		Port:     8081,
		Capacity: 10 * 1024 * 1024 * 1024, // gB
		Used:     0,
		Status:   common.NodeHealthy,
		LastSeen: time.Now(),
	}

	// Shutdown coordination
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errChan := make(chan error, 2)
	var wg sync.WaitGroup

	// Setup structured logging
	rootLogger, err := logging.InitLogger()
	if err != nil {
		log.Fatal(err.Error())
	}
	logger := logging.ExtendLogger(rootLogger, slog.String("node_id", datanodeInfo.ID))

	// Datanode server
	// ChunkStore implementation is chosen here
	chunkSerializer := encoding.NewProtoSerializer()

	chunkStore, err := chunk.NewChunkDiskStorage(appConfig.Node.DiskStorage, chunkSerializer, logger)
	if err != nil {
		log.Fatal(err.Error())
	}

	clusterStateManager := state.NewClusterStateManager()
	nodeSelector := cluster.NewNodeSelector(clusterStateManager)
	streamer := streamer.NewStreamer(appConfig.Node.Streamer)
	replicationManager := datanode.NewParalellReplicationService(appConfig.Node.Replication, streamer, logger)
	sessionManager := datanode.NewStreamingSessionManager(appConfig.Node.Session, logger)
	coordinatorFinder := state.NewCoordinatorFinder()

	server := datanode.NewDataNodeServer(&datanodeInfo, appConfig.Node, chunkStore, replicationManager,
		sessionManager, clusterStateManager, coordinatorFinder, nodeSelector, logger)

	// gRPC initialization
	grpcServer := grpc.NewServer()
	proto.RegisterDataNodeServiceServer(grpcServer, server)

	// Setup listener for gRPC server
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", datanodePort))
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer func() {
			if r := recover(); r != nil {
				logger.Error("Recovered from panic in gRPC server, writing to errChan...",
					slog.String("error", err.Error()))
				errChan <- fmt.Errorf("panic in gRPC server: %v", r)
			}
		}()

		logger.Info(fmt.Sprintf("Starting datanode gRPC server on :%d", datanodePort))
		if err := grpcServer.Serve(listener); err != nil {
			select {
			case <-ctx.Done():
				logger.Info("gRPC server stopped due to graceful shutdown")
			default:
				errChan <- fmt.Errorf("gRPC server failed: %w", err)
			}
		}
	}()

	// Cluster node setup
	nodeServices := datanode_services.NewNodeAgentServices(coordinatorFinder, datanode_services.NewRegisterService(logger))
	nodeControllers := datanode_controllers.NewNodeAgentControllers(datanode_controllers.NewHeartbeatController(ctx, appConfig.Agent.Heartbeat, logger))
	nodeAgent := cluster.NewNodeAgent(&appConfig.Agent, &datanodeInfo, clusterStateManager, coordinatorFinder, nodeServices, nodeControllers, logger)

	// Run node agent, which includes heartbeat loop
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := nodeAgent.Run(); err != nil {
			errChan <- fmt.Errorf("cluster node failed: %w", err)
		}
	}()

	// Graceful shutdown on SIGINT/SIGTERM
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	// Wait for shutdown signal or error
	select {
	case sig := <-sigCh:
		logger.Info(fmt.Sprintf("Received %s signal, initiating graceful shutdown...", sig.String()))
	case err := <-errChan:
		logger.Error("Received error, initiating shutdown", slog.String("error", err.Error()))
	}

	// Cancel context to signal all goroutines to stop
	// this will not stop the gRPC server as it is not context aware
	cancel()

	// Now, stop the gRPC server so we can wait for the wg signal that it is closed
	stopDone := make(chan struct{})
	go func() {
		grpcServer.GracefulStop()
		close(stopDone)
	}()

	// Give goroutines some time to finish gracefully
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	// Wait for goroutines to finish or timeout
	select {
	case <-done:
		logger.Info("All goroutines finished gracefully")
	case <-time.After(15 * time.Second):
		logger.Info("Timeout waiting for goroutines to finish, forcing shutdown")
	}

	select {
	case <-stopDone:
		logger.Info("gRPC server stopped gracefully")
	case <-time.After(15 * time.Second):
		logger.Info("Timeout waiting for gRPC server to stop gracefully, forcing stop")
		grpcServer.Stop()
	}

	log.Println("Server stopped, exiting...")
}

// Objective, reach this approach:
// func main() {
//     // 1. Load configuration
//     cfg := config.Load()
//
//     // 2. Create the datanode's specific service implementation
//     datanodeService := datanode.NewService(cfg.StoragePath)
//
//     // 3. Create the main cluster node object, injecting the service
//     clusterNode, err := cluster.NewNode(cfg.Cluster, datanodeService)
//     if err != nil {
//         log.Fatal("Failed to create cluster node", err)
//     }
//
//     // 4. Start the node (this starts gRPC, heartbeating, etc.)
//     // This call would block until the node is shut down.
//     if err := clusterNode.Run(); err != nil {
//         log.Fatal("Node runtime error", err)
//     }
// }
