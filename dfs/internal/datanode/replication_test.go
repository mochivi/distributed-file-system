package datanode

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mochivi/distributed-file-system/internal/config"
	"github.com/mochivi/distributed-file-system/pkg/proto"
	"github.com/mochivi/distributed-file-system/pkg/streaming"
	"github.com/mochivi/distributed-file-system/pkg/testutils"
	"google.golang.org/grpc"

	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/pkg/client_pool"
)

// stubDataNodeReplicationServer is a lightweight gRPC server implementation that mimics
// only the behaviour required by the ReplicationManager unit tests.
// The behaviour can be tweaked through the exported fields.
type stubDataNodeReplicationServer struct {
	proto.UnimplementedDataNodeServiceServer

	// Determines whether the server should accept or reject the replication
	// handshake in PrepareChunkUpload.
	accept bool

	// Determines whether the per-chunk/terminal acknowledgements sent back on
	// the UploadChunkStream carry Success=true or false.
	ackSuccess bool
}

func (s *stubDataNodeReplicationServer) PrepareChunkUpload(ctx context.Context, req *proto.UploadChunkRequest) (*proto.NodeReady, error) {
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
func (s *stubDataNodeReplicationServer) UploadChunkStream(stream grpc.BidiStreamingServer[proto.ChunkDataStream, proto.ChunkDataAck]) error {
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

// --- Tests ----------------------------------------------

func TestReplicationManager_replicate(t *testing.T) {
	header, data := testutils.NewRandomChunk(128)

	cfg := config.ParallelReplicationServiceConfig{ReplicateTimeout: time.Second * 2}
	streamer := streaming.NewClientStreamer(config.StreamerConfig{
		MaxChunkRetries:  1,
		ChunkStreamSize:  1024,
		BackpressureTime: time.Millisecond * 10,
		WaitReplicas:     false,
	})
	rm := NewParalellReplicationService(cfg, streamer, slog.New(slog.NewTextHandler(io.Discard, nil)))

	tests := []struct {
		name        string
		accept      bool
		ackSuccess  bool
		expectError bool
	}{
		{name: "successful replication", accept: true, ackSuccess: true, expectError: false},
		{name: "stream failure", accept: true, ackSuccess: false, expectError: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stubServer := &stubDataNodeReplicationServer{accept: tt.accept, ackSuccess: tt.ackSuccess}
			datanodeClient, cleanup := testutils.NewTestDataNodeClientWithStubServer(t, stubServer)
			defer cleanup()

			err := rm.replicate(context.Background(), datanodeClient, header, "test-session", data, rm.logger)
			if (err != nil) != tt.expectError {
				t.Fatalf("expected error=%v, got %v", tt.expectError, err)
			}
		})
	}
}

func TestReplicationManager_replicateToClient(t *testing.T) {
	header, data := testutils.NewRandomChunk(128)

	cfg := config.ParallelReplicationServiceConfig{ReplicateTimeout: time.Second * 2}
	streamer := streaming.NewClientStreamer(config.StreamerConfig{
		MaxChunkRetries:  1,
		ChunkStreamSize:  1024,
		BackpressureTime: time.Millisecond * 10,
		WaitReplicas:     false,
	})
	rm := NewParalellReplicationService(cfg, streamer, slog.New(slog.NewTextHandler(io.Discard, nil)))

	tests := []struct {
		name        string
		accept      bool
		ackSuccess  bool
		expectError bool
	}{
		{name: "successful client replication", accept: true, ackSuccess: true, expectError: false},
		{name: "client replication failure", accept: true, ackSuccess: false, expectError: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, cleanup := testutils.NewTestDataNodeClientWithStubServer(t, &stubDataNodeReplicationServer{
				accept:     tt.accept,
				ackSuccess: tt.ackSuccess,
			})
			defer cleanup()

			// Test data structures
			replicatedNodes := &ReplicatedNodes{}
			semaphore := make(chan struct{}, 1)
			errChan := make(chan error, 1)
			var wg sync.WaitGroup
			var acceptedCount atomic.Int64
			var activeReplications atomic.Int64

			// Put something in semaphore so the function can release it
			semaphore <- struct{}{}

			// Call the function directly (this would normally be called as a goroutine)
			wg.Add(1)
			rm.replicateToClient(context.Background(), client, "test-session", header, data, rm.logger, &wg, semaphore, errChan, replicatedNodes, &acceptedCount, &activeReplications)

			wg.Wait()
			close(errChan)

			// Check if errors were sent to errChan
			var gotError bool
			for err := range errChan {
				if err != nil {
					gotError = true
				}
			}

			if tt.expectError && !gotError {
				t.Fatal("expected error but got none")
			}
			if !tt.expectError && gotError {
				t.Fatal("unexpected error")
			}

			// Check if node was added on success
			nodes := replicatedNodes.GetNodes()
			if !tt.expectError && len(nodes) != 1 {
				t.Fatalf("expected 1 replicated node, got %d", len(nodes))
			}
			if tt.expectError && len(nodes) != 0 {
				t.Fatalf("expected 0 replicated nodes on error, got %d", len(nodes))
			}

			// Check accepted count
			if !tt.expectError && acceptedCount.Load() != 1 {
				t.Fatalf("expected accepted count 1, got %d", acceptedCount.Load())
			}
			if tt.expectError && acceptedCount.Load() != 0 {
				t.Fatalf("expected accepted count 0 on error, got %d", acceptedCount.Load())
			}
		})
	}
}

func TestReplicationManager_waitAndValidateResults(t *testing.T) {
	cfg := config.ParallelReplicationServiceConfig{ReplicateTimeout: time.Second * 2}
	streamer := streaming.NewClientStreamer(config.StreamerConfig{
		MaxChunkRetries:  1,
		ChunkStreamSize:  1024,
		BackpressureTime: time.Millisecond * 10,
		WaitReplicas:     false,
	})
	rm := NewParalellReplicationService(cfg, streamer, slog.New(slog.NewTextHandler(io.Discard, nil)))

	tests := []struct {
		name             string
		acceptedCount    int64
		requiredReplicas int
		errorsToAdd      int
		expectError      bool
	}{
		{
			name:             "sufficient replicas",
			acceptedCount:    3,
			requiredReplicas: 2,
			errorsToAdd:      0,
			expectError:      false,
		},
		{
			name:             "exactly required replicas",
			acceptedCount:    2,
			requiredReplicas: 2,
			errorsToAdd:      0,
			expectError:      false,
		},
		{
			name:             "insufficient replicas",
			acceptedCount:    1,
			requiredReplicas: 3,
			errorsToAdd:      2,
			expectError:      true,
		},
		{
			name:             "zero replicas",
			acceptedCount:    0,
			requiredReplicas: 1,
			errorsToAdd:      1,
			expectError:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var wg sync.WaitGroup
			var acceptedCount atomic.Int64
			acceptedCount.Store(tt.acceptedCount)

			errChan := make(chan error, 10)
			replicatedNodes := &ReplicatedNodes{}

			// Add some nodes if we have accepted replicas
			for i := int64(0); i < tt.acceptedCount; i++ {
				// Create mock clients to get proper NodeInfo
				client, cleanup := testutils.NewTestDataNodeClientWithStubServer(t, &stubDataNodeReplicationServer{
					accept:     true,
					ackSuccess: true,
				})
				replicatedNodes.AddNode(client.Node())
				cleanup()
			}

			// Add errors to the channel
			for i := 0; i < tt.errorsToAdd; i++ {
				errChan <- io.ErrUnexpectedEOF
			}

			result, err := rm.waitAndValidateResults(&wg, errChan, &acceptedCount, tt.requiredReplicas, replicatedNodes)

			if tt.expectError && err == nil {
				t.Fatal("expected error but got none")
			}
			if !tt.expectError && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !tt.expectError && result == nil {
				t.Fatal("expected result but got nil")
			}
		})
	}
}

// Integration test that uses real clients and a real client pool
func TestReplicationManager_Replicate_Integration(t *testing.T) {
	header, data := testutils.NewRandomChunk(128)

	cfg := config.ParallelReplicationServiceConfig{ReplicateTimeout: time.Second * 5}
	streamer := streaming.NewClientStreamer(config.StreamerConfig{
		MaxChunkRetries:  1,
		ChunkStreamSize:  1024,
		BackpressureTime: time.Millisecond * 10,
		WaitReplicas:     false,
	})
	rm := NewParalellReplicationService(cfg, streamer, slog.New(slog.NewTextHandler(io.Discard, nil)))

	tests := []struct {
		name             string
		requiredReplicas int
		setupClientPool  func(t *testing.T) (client_pool.ClientPool, func())
		expectError      bool
		expectedNodes    int
	}{
		{
			name:             "success: replication with sufficient clients",
			requiredReplicas: 2,
			setupClientPool: func(t *testing.T) (client_pool.ClientPool, func()) {
				return createClientPool(t, 2, [][]bool{{true, true}, {true, true}})
			},
			expectError:   false,
			expectedNodes: 2,
		},
		{
			name:             "success: exact required replicas",
			requiredReplicas: 3,
			setupClientPool: func(t *testing.T) (client_pool.ClientPool, func()) {
				return createClientPool(t, 3, [][]bool{{true, true}, {true, true}, {true, true}})
			},
			expectError:   false,
			expectedNodes: 3,
		},
		{
			name:             "error: insufficient clients in pool",
			requiredReplicas: 3,
			setupClientPool: func(t *testing.T) (client_pool.ClientPool, func()) {
				return createClientPool(t, 2, [][]bool{{true, true}, {true, true}})
			},
			expectError:   true,
			expectedNodes: 0,
		},
		{
			name:             "error: all clients reject connection",
			requiredReplicas: 2,
			setupClientPool: func(t *testing.T) (client_pool.ClientPool, func()) {
				return createClientPool(t, 3, [][]bool{{false, true}, {false, true}, {false, true}})
			},
			expectError:   true,
			expectedNodes: 0,
		},
		{
			name:             "error: streaming failures after connection",
			requiredReplicas: 2,
			setupClientPool: func(t *testing.T) (client_pool.ClientPool, func()) {
				return createClientPool(t, 3, [][]bool{{true, false}, {true, false}, {true, false}})
			},
			expectError:   true,
			expectedNodes: 0,
		},
		{
			name:             "success: mixed success and failure",
			requiredReplicas: 2,
			setupClientPool: func(t *testing.T) (client_pool.ClientPool, func()) {
				return createClientPool(t, 4, [][]bool{{true, true}, {true, true}, {false, true}, {false, true}})
			},
			expectError:   false,
			expectedNodes: 2,
		},
		{
			name:             "success: mixed success and failure with different ordering",
			requiredReplicas: 2,
			setupClientPool: func(t *testing.T) (client_pool.ClientPool, func()) {
				return createClientPool(t, 4, [][]bool{{true, true}, {false, true}, {true, true}, {false, true}})
			},
			expectError:   false,
			expectedNodes: 2,
		},
		{
			name:             "error: streaming failures after connection",
			requiredReplicas: 2,
			setupClientPool: func(t *testing.T) (client_pool.ClientPool, func()) {
				return createClientPool(t, 3, [][]bool{{true, false}, {true, false}, {true, false}})
			},
			expectError:   true,
			expectedNodes: 0,
		},
		{
			name:             "success: mixed success and failure",
			requiredReplicas: 2,
			setupClientPool: func(t *testing.T) (client_pool.ClientPool, func()) {
				return createClientPool(t, 4, [][]bool{{true, true}, {false, true}, {true, true}, {false, true}})
			},
			expectError:   false,
			expectedNodes: 2,
		},
		{
			name:             "success: single replica required",
			requiredReplicas: 1,
			setupClientPool: func(t *testing.T) (client_pool.ClientPool, func()) {
				return createClientPool(t, 2, [][]bool{{true, true}, {true, true}})
			},
			expectError:   false,
			expectedNodes: 1,
		},
		{
			name:             "success: single replica required, first few fail",
			requiredReplicas: 1,
			setupClientPool: func(t *testing.T) (client_pool.ClientPool, func()) {
				return createClientPool(t, 4, [][]bool{{false, true}, {true, false}, {true, false}, {true, true}})
			},
			expectError:   false,
			expectedNodes: 1,
		},
		{
			name:             "success: single replica required, first few fail,  more replicas required",
			requiredReplicas: 3,
			setupClientPool: func(t *testing.T) (client_pool.ClientPool, func()) {
				return createClientPool(t, 8, [][]bool{
					{false, true}, {true, false}, {true, false}, {true, true},
					{false, true}, {true, false}, {true, true}, {true, true},
				})
			},
			expectError:   false,
			expectedNodes: 3,
		},
		{
			name:             "success: single replica required, first few fail",
			requiredReplicas: 1,
			setupClientPool: func(t *testing.T) (client_pool.ClientPool, func()) {
				return createClientPool(t, 4, [][]bool{{false, true}, {true, false}, {true, false}, {false, true}})
			},
			expectError:   true,
			expectedNodes: 0,
		},
		{
			name:             "success: large number of replicas",
			requiredReplicas: 5,
			setupClientPool: func(t *testing.T) (client_pool.ClientPool, func()) {
				return createClientPool(t, 7, [][]bool{
					{true, true}, {true, true}, {true, true}, {true, true}, {true, true},
					{true, true}, {true, true},
				})
			},
			expectError:   false,
			expectedNodes: 5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clientPool, cleanup := tt.setupClientPool(t)
			defer cleanup()

			// In the test, add logging to see which clients are being tried
			nodes, err := rm.Replicate(clientPool, header, data, tt.requiredReplicas)

			if tt.expectError {
				if err == nil {
					t.Fatal("expected error but got none")
				}
				t.Logf("expected error: %v", err)
			} else {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}

				if len(nodes) != tt.expectedNodes {
					t.Fatalf("expected %d replicated nodes, got %d", tt.expectedNodes, len(nodes))
				}

				// Verify all nodes are healthy
				for _, node := range nodes {
					if node.Status != common.NodeHealthy {
						t.Errorf("expected node %s to be healthy, got %v", node.ID, node.Status)
					}
				}
			}
		})
	}
}

func createClientPool(t *testing.T, numClients int, responses [][]bool) (client_pool.ClientPool, func()) {
	t.Helper()

	nodeInfos := make([]*common.NodeInfo, 0, numClients)
	cleanups := make([]func(), numClients)

	for i := range numClients {
		client, cleanup := testutils.NewTestDataNodeClientWithStubServer(t, &stubDataNodeReplicationServer{
			accept:     responses[i][0],
			ackSuccess: responses[i][1],
		})

		nodeInfos = append(nodeInfos, client.Node())
		cleanups[i] = cleanup
	}

	pool, err := client_pool.NewRotatingClientPool(nodeInfos)
	if err != nil {
		for _, cleanup := range cleanups {
			cleanup()
		}
		t.Fatalf("failed to create client pool: %v", err)
	}

	return pool, func() {
		pool.Close()
		for _, cleanup := range cleanups {
			cleanup()
		}
	}
}
