package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/mochivi/distributed-file-system/internal/coordinator"
	"github.com/mochivi/distributed-file-system/internal/storage/metadata"
	"github.com/mochivi/distributed-file-system/pkg/proto"
	"google.golang.org/grpc"
)

func main() {
	cfg := coordinator.DefaultCoordinatorConfig()

	metadataStore := metadata.NewMetadataLocalStorage()
	metadataManager := coordinator.NewMetadataManager(cfg.Metadata.CommitTimeout)

	// Create coordinator server
	server := coordinator.NewCoordinator(metadataStore, metadataManager, cfg)

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
				log.Printf("Recovered from panic in gRPC server: %v", r)
				grpcServer.GracefulStop()
			}
		}()

		log.Printf("Starting coordinator gRPC server on :%d", cfg.Port)
		if err := grpcServer.Serve(listener); err != nil {
			log.Fatalf("Failed to server gRPC server: %v", err)
		}
	}()

	// Graceful shutdown on SIGINT/SIGTERM
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
	<-ch

	log.Println("Stopping coordinator gRPC server...")
	grpcServer.GracefulStop()
	log.Println("Coordinator server stopped, exiting...")
}
