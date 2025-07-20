package launcher

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc"
)

type GRPCSetupFunc func(*sync.WaitGroup, chan error) (*grpc.Server, net.Listener)
type AgentLaunchFunc func(*sync.WaitGroup, chan error)

// Launch coordinates starting a gRPC server and node agent with graceful shutdown handling.
// It uses the provided functions to initialize and run the server/agent.
func Launch(ctx context.Context, cancel context.CancelFunc, logger *slog.Logger,
	grpcSetup GRPCSetupFunc, agentLaunch AgentLaunchFunc, shutdownTimeout time.Duration) error {

	// defer cancel()
	errChan := make(chan error, 2)
	var wg sync.WaitGroup

	// Start gRPC server
	grpcServer, listener := grpcSetup(&wg, errChan)
	port := listener.Addr().(*net.TCPAddr).Port
	launchServer(ctx, &wg, errChan, grpcServer, listener, logger, port)

	// Start node agent
	agentLaunch(&wg, errChan)

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

	// Cancel context to signal context-aware goroutines to stop
	cancel()

	// Stop gRPC server gracefully
	stopDone := make(chan struct{})
	go func() {
		grpcServer.GracefulStop()
		close(stopDone)
	}()

	// Wait for all goroutines to finish
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	// Wait with timeout
	select {
	case <-done:
		logger.Info("All goroutines finished gracefully")
	case <-time.After(shutdownTimeout):
		logger.Info("Timeout waiting for goroutines to finish, forcing shutdown")
	}

	// Ensure gRPC server is stopped
	select {
	case <-stopDone:
		logger.Info("gRPC server stopped gracefully")
	case <-time.After(shutdownTimeout):
		logger.Info("Timeout waiting for gRPC server to stop gracefully, forcing stop")
		grpcServer.Stop()
	}

	// Close listener
	if err := listener.Close(); err != nil {
		logger.Error("Failed to close listener", slog.String("error", err.Error()))
	}

	logger.Info("Server stopped")
	return nil
}

func launchServer(ctx context.Context, wg *sync.WaitGroup, errChan chan error, grpcServer *grpc.Server,
	listener net.Listener, logger *slog.Logger, port int) {
	wg.Add(1)

	go func() {
		defer wg.Done()
		defer func() {
			if r := recover(); r != nil {
				logger.Error("Recovered from panic in gRPC server", slog.String("error", fmt.Sprintf("%v", r)))
				grpcServer.GracefulStop()
			}
		}()

		logger.Info(fmt.Sprintf("Starting gRPC server on :%d", port))
		if err := grpcServer.Serve(listener); err != nil {
			select {
			case <-ctx.Done():
				logger.Info("gRPC server stopped due to graceful shutdown")
			default:
				errChan <- fmt.Errorf("gRPC server failed: %w", err)
			}
		}
	}()
}
