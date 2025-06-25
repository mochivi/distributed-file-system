package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"io/fs"

	"github.com/joho/godotenv"
	"github.com/mochivi/distributed-file-system/internal/cluster"
	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/internal/datanode"
	"github.com/mochivi/distributed-file-system/internal/storage/chunk"
	"github.com/mochivi/distributed-file-system/internal/storage/encoding"
	"github.com/mochivi/distributed-file-system/pkg/logging"
	"github.com/mochivi/distributed-file-system/pkg/proto"
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
	if err := godotenv.Load(); err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			log.Fatalf("failed to load environment: %v", err)
		}
	}

	// Shutdown coordination
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errChan := make(chan error, 2)
	var wg sync.WaitGroup

	// Create node config
	nodeConfig := datanode.DefaultDatanodeConfig()

	// Setup structured logging
	rootLogger, err := logging.InitLogger()
	if err != nil {
		log.Fatal(err.Error())
	}
	logger := logging.ExtendLogger(rootLogger, slog.String("node_id", nodeConfig.Info.ID))

	// Datanode server
	server, err := initServer(nodeConfig, logger)
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

		logger.Info(fmt.Sprintf("Starting datanode gRPC server on :%d", server.Config.Info.Port))
		if err := grpcServer.Serve(listener); err != nil {
			select {
			case <-ctx.Done():
				logger.Info("gRPC server stopped due to graceful shutdown")
			default:
				errChan <- fmt.Errorf("gRPC server failed: %w", err)
			}
		}
	}()

	// Register datanode with coordinator - if fails, should crash
	if err := server.NodeManager.BootstrapCoordinatorNode(); err != nil {
		logger.Error("Failed to bootstrap coordinator node", slog.String("error", err.Error()))
		os.Exit(1)
	}

	// Register datanode with coordinator
	if err := server.RegisterWithCoordinator(ctx); err != nil {
		logger.Error("Failed to register with coordinator", slog.String("error", err.Error()))
	}

	// Start heartbeat loop
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer func() {
			if r := recover(); r != nil {
				errChan <- fmt.Errorf("panic in heartbeat loop: %v", r)
			}
		}()

		// Create a context with timeout for heartbeat operations
		heartbeatCtx, heartbeatCancel := context.WithCancel(ctx)
		defer heartbeatCancel()

		logger.Info("Starting heartbeat loop...")
		if err := server.HeartbeatLoop(heartbeatCtx); err != nil {
			// Only send error if it's not due to context cancellation
			select {
			case <-ctx.Done():
				logger.Info("Heartbeat loop stopped due to context cancellation")
			default:
				errChan <- fmt.Errorf("heartbeat loop failed: %w", err)
			}
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

func initServer(nodeConfig datanode.DataNodeConfig, logger *slog.Logger) (*datanode.DataNodeServer, error) {
	// ChunkStore implementation is chosen here
	chunkSerializer := encoding.NewProtoSerializer()

	chunkStore, err := chunk.NewChunkDiskStorage(nodeConfig.DiskStorage, chunkSerializer, logger)
	if err != nil {
		log.Fatal(err.Error())
	}

	// Datanode dependencies
	nodeSelector := common.NewNodeSelector()
	nodeManager := cluster.NewNodeManager(nodeSelector)
	streamer := common.NewStreamer(common.DefaultStreamerConfig())
	replicationManager := datanode.NewReplicationManager(nodeConfig.Replication, streamer, logger)
	sessionManager := datanode.NewSessionManager()

	return datanode.NewDataNodeServer(chunkStore, replicationManager, sessionManager, nodeManager, nodeConfig, logger), nil
}
