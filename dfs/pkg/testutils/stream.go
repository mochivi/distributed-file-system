package testutils

import (
	"github.com/mochivi/distributed-file-system/pkg/proto"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc"
)

// MockBidiStreamClient is a mock for the grpc.BidiStreamingClient interface.
type MockBidiStreamClient struct {
	grpc.ClientStream
	mock.Mock
}

func (s *MockBidiStreamClient) Send(m *proto.ChunkDataStream) error {
	args := s.Called(m)
	return args.Error(0)
}

func (s *MockBidiStreamClient) Recv() (*proto.ChunkDataAck, error) {
	args := s.Called()
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*proto.ChunkDataAck), args.Error(1)
}

func (s *MockBidiStreamClient) CloseSend() error {
	args := s.Called()
	return args.Error(0)
}

// MockBidiStreamServer is a mock for the grpc.BidiStreamingServer interface.
type MockBidiStreamServer struct {
	grpc.ServerStream
	mock.Mock
}

func (s *MockBidiStreamServer) Send(m *proto.ChunkDataAck) error {
	args := s.Called(m)
	return args.Error(0)
}

func (s *MockBidiStreamServer) Recv() (*proto.ChunkDataStream, error) {
	args := s.Called()
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*proto.ChunkDataStream), args.Error(1)
}

// MockStreamClient is a mock for the grpc.ServerStreamingClient[proto.ChunkDataStream] interface.
type MockStreamClient struct {
	grpc.ServerStreamingClient[proto.ChunkDataStream]
	mock.Mock
}

func (s *MockStreamClient) Recv() (*proto.ChunkDataStream, error) {
	args := s.Called()
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*proto.ChunkDataStream), args.Error(1)
}

// MockServerStreamAck is a mock for the grpc.ServerStreamingClient[proto.ChunkDataAck] interface.
type MockStreamServer struct {
	grpc.ServerStreamingServer[proto.ChunkDataStream]
	mock.Mock
}

func (s *MockStreamServer) Send(m *proto.ChunkDataStream) error {
	args := s.Called(m)
	return args.Error(0)
}
