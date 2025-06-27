package datanode

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/mochivi/distributed-file-system/internal/clients"
	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/internal/config"
	"github.com/mochivi/distributed-file-system/pkg/proto"
	"google.golang.org/grpc"
)

// stubDataNodeServer is a lightweight gRPC server implementation that mimics
// only the behaviour required by the ReplicationManager unit tests.
// The behaviour can be tweaked through the exported fields.
type stubDataNodeServer struct {
	proto.UnimplementedDataNodeServiceServer

	// Determines whether the server should accept or reject the replication
	// handshake in PrepareChunkUpload.
	accept bool

	// Determines whether the per-chunk/terminal acknowledgements sent back on
	// the UploadChunkStream carry Success=true or false.
	ackSuccess bool
}

func (s *stubDataNodeServer) PrepareChunkUpload(ctx context.Context, req *proto.UploadChunkRequest) (*proto.NodeReady, error) {
	return &proto.NodeReady{
		Accept:    s.accept,
		Message:   "stub server response",
		SessionId: "test-session",
	}, nil
}

// UploadChunkStream implements a very small subset of the real behaviour. It
// reads all incoming ChunkDataStream messages, keeps track of the cumulative
// bytes received and sends back an acknowledgement for every message. When the
// client signals the end of the stream (io.EOF), a final acknowledgement is
// sent and the call returns nil.
func (s *stubDataNodeServer) UploadChunkStream(stream grpc.BidiStreamingServer[proto.ChunkDataStream, proto.ChunkDataAck]) error {
	var total int64

	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			// final ack
			ack := &proto.ChunkDataAck{
				SessionId:     "test-session",
				Success:       s.ackSuccess,
				Message:       "finished",
				BytesReceived: total,
				ReadyForNext:  true,
			}
			_ = stream.Send(ack) // ignore send error – client side will catch it
			return nil
		}
		if err != nil {
			return err
		}

		total += int64(len(msg.GetData()))
		ack := &proto.ChunkDataAck{
			SessionId:     msg.GetSessionId(),
			Success:       s.ackSuccess,
			Message:       "ack",
			BytesReceived: total,
			ReadyForNext:  true,
		}
		if err := stream.Send(ack); err != nil {
			return err
		}
	}
}

// --- Tests ------------------------------------------------------------------

// replicate test
// aimed at another datanode, should replicate the chunk
func TestReplicationManager_replicate(t *testing.T) {
	header, data := newRandomChunk(128)

	// Common replication manager used across sub-tests.
	cfg := config.ReplicateManagerConfig{ReplicateTimeout: time.Second * 2}
	streamer := common.NewStreamer(common.StreamerConfig{
		MaxChunkRetries:  1,
		ChunkStreamSize:  1024,
		BackpressureTime: time.Millisecond * 10,
		WaitReplicas:     false, // If true, node will attempt to replicate the chunk.
	})
	rm := NewReplicationManager(cfg, streamer, slog.New(slog.NewTextHandler(io.Discard, nil)))

	tests := []struct {
		name        string
		accept      bool
		ackSuccess  bool
		expectError bool
	}{
		// {name: "successful replication", accept: true, ackSuccess: true, expectError: false},
		// {name: "handshake rejected", accept: false, ackSuccess: true, expectError: true},
		{name: "stream failure", accept: true, ackSuccess: false, expectError: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			datanodeClient, cleanup := NewTestDataNodeClientWithStubServer(t, &stubDataNodeServer{accept: tt.accept, ackSuccess: tt.ackSuccess})
			defer cleanup()

			err := rm.replicate(context.Background(), datanodeClient, header, data, rm.logger)
			if (err != nil) != tt.expectError {
				t.Fatalf("expected error=%v, got %v", tt.expectError, err)
			}
		})
	}
}

func TestReplicationManager_paralellReplicate(t *testing.T) {
	header, data := newRandomChunk(128)

	// Common replication manager used across sub-tests.
	cfg := config.ReplicateManagerConfig{ReplicateTimeout: time.Second * 2}
	streamer := common.NewStreamer(common.StreamerConfig{
		MaxChunkRetries:  1,
		ChunkStreamSize:  1024,
		BackpressureTime: time.Millisecond * 10,
		WaitReplicas:     false,
	})
	rm := NewReplicationManager(cfg, streamer, slog.New(slog.NewTextHandler(io.Discard, nil)))

	tests := []struct {
		name             string
		requiredReplicas int
		nClients         int
		accept           bool
		ackSuccess       bool
		expectError      bool
		expectedNodes    int
	}{
		{
			name:             "successful parallel replication",
			nClients:         2,
			requiredReplicas: 2,
			accept:           true,
			ackSuccess:       true,
			expectError:      false,
			expectedNodes:    2,
		},
		{
			name:             "insufficient replicas",
			nClients:         2,
			requiredReplicas: 5, // larger than there are clients available
			accept:           true,
			ackSuccess:       true,
			expectError:      true,
			expectedNodes:    0,
		},
		{
			name:             "handshake rejected",
			nClients:         2,
			requiredReplicas: 1,
			accept:           false,
			ackSuccess:       true,
			expectError:      true,
			expectedNodes:    0,
		},
		{
			name:             "stream failure",
			nClients:         2,
			requiredReplicas: 1,
			accept:           true,
			ackSuccess:       false,
			expectError:      true,
			expectedNodes:    0,
		},
		{
			name:             "no nodes provided",
			nClients:         0,
			requiredReplicas: 1,
			accept:           true,
			ackSuccess:       true,
			expectError:      true,
			expectedNodes:    0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create test clients with stub servers
			var testClients []*clients.DataNodeClient
			var cleanups []func()
			for i := 0; i < tt.nClients; i++ {
				client, cleanup := NewTestDataNodeClientWithStubServer(t, &stubDataNodeServer{
					accept:     tt.accept,
					ackSuccess: tt.ackSuccess,
				})
				testClients = append(testClients, client)
				cleanups = append(cleanups, cleanup)
			}

			// Cleanup all clients
			defer func() {
				for _, cleanup := range cleanups {
					cleanup()
				}
			}()

			// Test parallel replication
			replicatedNodes, err := rm.paralellReplicate(testClients, header, data, tt.requiredReplicas)

			// Check error expectation
			if (err != nil) != tt.expectError {
				t.Fatalf("expected error=%v, got %v", tt.expectError, err)
			}

			// Check number of replicated nodes
			if len(replicatedNodes) != tt.expectedNodes {
				t.Fatalf("expected %d replicated nodes, got %d", tt.expectedNodes, len(replicatedNodes))
			}

			// If no error expected, verify the nodes are properly replicated
			if !tt.expectError && len(replicatedNodes) > 0 {
				for _, node := range replicatedNodes {
					if node == nil {
						t.Fatal("replicated node is nil")
					}
				}
			}
		})
	}
}
