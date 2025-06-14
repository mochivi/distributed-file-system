package main

import (
	"fmt"
	"log"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/internal/coordinator"
	"github.com/mochivi/distributed-file-system/internal/storage/metadata"
	"github.com/mochivi/distributed-file-system/pkg/logging"
	"github.com/mochivi/distributed-file-system/pkg/proto"
	"google.golang.org/grpc"
)

func main() {
	cfg := coordinator.DefaultCoordinatorConfig()

	// Coordinator dependencies

	rootLogger, err := logging.InitLogger()
	if err != nil {
		log.Fatalf("Failed to initialize logger: %v", err)
	}
	logger := logging.ExtendLogger(rootLogger, slog.String("node_id", cfg.ID))

	metadataStore := metadata.NewMetadataLocalStorage()
	metadataManager := coordinator.NewMetadataManager(cfg.Metadata.CommitTimeout, logger)
	nodeSelector := common.NewNodeSelector()
	nodeManager := common.NewNodeManager(nodeSelector)

	// Create coordinator server
	server := coordinator.NewCoordinator(cfg, metadataStore, metadataManager, nodeManager, logger)

	// gRPC server and register
	grpcServer := grpc.NewServer()
	proto.RegisterCoordinatorServiceServer(grpcServer, server)

	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.Port))
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	go func() {
		defer func() {
			if r := recover(); r != nil {
				logger.Error("Recovered from panic in gRPC server", slog.String("error", fmt.Sprintf("%v", r)))
				grpcServer.GracefulStop()
			}
		}()

		logger.Info(fmt.Sprintf("Starting coordinator gRPC server on :%d", cfg.Port))
		if err := grpcServer.Serve(listener); err != nil {
			logger.Error("Failed to server gRPC server", slog.String("error", err.Error()))
		}
	}()

	// Graceful shutdown on SIGINT/SIGTERM
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
	<-ch

	logger.Info("Stopping coordinator gRPC server...")
	grpcServer.GracefulStop()
	logger.Info("Coordinator server stopped, exiting...")
}
