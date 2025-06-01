package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/mochivi/distributed-file-system/internal/datanode"
	"github.com/mochivi/distributed-file-system/internal/storage/chunk"
	"github.com/mochivi/distributed-file-system/pkg/proto"
	"google.golang.org/grpc"
)

const DATA_NODE_PORT = "4040"

func main() {

	cwd, err := os.Getwd()
	if err != nil {
		log.Fatalf("unable to get cwd: %v", err)
	}
	rootDDir := fmt.Sprintf("%s%schunks", cwd, filepath.Separator)
	chunkStore := chunk.NewChunkDiskStorage(rootDDir)
	replicationManager := datanode.ReplicationManager{}

	server := datanode.NewDataNodeServer(chunkStore, &replicationManager)
	grpcServer := grpc.NewServer()

	proto.RegisterDataNodeServiceServer(grpcServer, server)

	listener, err := net.Listen("tcp", DATA_NODE_PORT)
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

		log.Printf("Starting gRPC server on :%s", DATA_NODE_PORT)
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
