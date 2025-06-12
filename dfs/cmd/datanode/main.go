package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

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
	replicationManager := datanode.NewReplicationManager(nodeConfig.Replication)
	sessionManager := datanode.NewSessionManager()

	return datanode.NewDataNodeServer(chunkStore, replicationManager, sessionManager, nodeManager, nodeConfig), nil
}

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
		log.Fatalf("failed to load environment: %v", err)
	}

	// Shutdown coordination
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errChan := make(chan error, 2)
	var wg sync.WaitGroup

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

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer func() {
			if r := recover(); r != nil {
				log.Printf("Recovered from panic in gRPC server, writing to errChan...: %v", r)
				errChan <- fmt.Errorf("panic in gRPC server: %v", r)
			}
		}()

		log.Printf("Starting datanode gRPC server on :%d", server.Config.Info.Port)
		if err := grpcServer.Serve(listener); err != nil {
			select {
			case <-ctx.Done():
				log.Println("gRPC server stopped due to graceful shutdown")
			default:
				errChan <- fmt.Errorf("gRPC server failed: %w", err)
			}
		}
	}()

	// Register datanode with coordinator - if fails, should crash
	coordinatorHost := utils.GetEnvString("COORDINATOR_HOST", "coordinator")
	coordinatorPort := utils.GetEnvInt("COORDINATOR_PORT", 8080)
	coordinatorAddress := fmt.Sprintf("%s:%d", coordinatorHost, coordinatorPort)

	// Temporarily, overwrite the datanode IP address with the docker dns name
	// server.Config.Info.IPAddress = "datanode"

	// Register datanode with coordinator
	if err := server.RegisterWithCoordinator(ctx, coordinatorAddress); err != nil {
		log.Fatalf("Failed to register with coordinator: %v", err)
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

		if err := server.HeartbeatLoop(heartbeatCtx, coordinatorAddress); err != nil {
			// Only send error if it's not due to context cancellation
			select {
			case <-ctx.Done():
				log.Println("Heartbeat loop stopped due to context cancellation")
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
		log.Printf("Received %s signal, initiating graceful shutdown...", sig.String())
	case err := <-errChan:
		log.Printf("Received error, initiating shutdown: %v", err)
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
		log.Println("All goroutines finished gracefully")
	case <-time.After(15 * time.Second):
		log.Println("Timeout waiting for goroutines to finish, forcing shutdown")
	}

	select {
	case <-stopDone:
		log.Println("gRPC server stopped gracefully")
	case <-time.After(15 * time.Second):
		log.Println("Timeout waiting for gRPC server to stop gracefully, forcing stop")
		grpcServer.Stop()
	}

	log.Println("Server stopped, exiting...")
}
