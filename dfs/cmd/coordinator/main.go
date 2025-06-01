package main

import (
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

const (
	REPLICATION_DEFAULT    = 3
	COORDINATOR_PORT       = "5244"
	DEFAULT_COMMIT_TIMEOUT = 15 // max time to wait for replication to commit metadata
)

func main() {
	metadataStore := metadata.NewMetadataLocalStorage()
	metadataManager := coordinator.NewMetadataManager(DEFAULT_COMMIT_TIMEOUT)
	server := coordinator.NewCoordinator(metadataStore, metadataManager, REPLICATION_DEFAULT)
	grpcServer := grpc.NewServer()

	proto.RegisterCoordinatorServiceServer(grpcServer, server)

	listener, err := net.Listen("tcp", COORDINATOR_PORT)
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

		log.Printf("Starting gRPC server on :%s", COORDINATOR_PORT)
		if err := grpcServer.Serve(listener); err != nil {
			log.Fatalf("Failed to server gRPC server: %v", err)
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
