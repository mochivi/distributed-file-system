package coordinator

import "google.golang.org/grpc"

type Server struct {
	grpcServer *grpc.Server
}
