package main

import (
	"github.com/mochivi/distributed-file-system/internal/coordinator"
	"github.com/mochivi/distributed-file-system/internal/storage/local"
	"github.com/mochivi/distributed-file-system/pkg/proto"
	"google.golang.org/grpc"
)

func main() {
	metadataStore := local.NewMetadataLocalStorage()
	server := coordinator.NewService(metadataStore)
	grpcServer := grpc.NewServer()

	proto.RegisterCoordinatorServiceServer(grpcServer, server)
}
