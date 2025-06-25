package datanode

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/mochivi/distributed-file-system/internal/cluster"
	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/internal/coordinator"
	"github.com/mochivi/distributed-file-system/pkg/logging"
	"github.com/mochivi/distributed-file-system/pkg/proto"
	"github.com/stretchr/testify/mock"
)

// Heartbeat stub server
type stubCoordinatorHeartbeatServer struct {
	proto.UnimplementedCoordinatorServiceServer
	hbResp coordinator.HeartbeatResponse // pre-programmed response
	hbErr  error
}

func (s *stubCoordinatorHeartbeatServer) DataNodeHeartbeat(ctx context.Context, req *proto.HeartbeatRequest) (*proto.HeartbeatResponse, error) {
	if s.hbErr != nil {
		return nil, s.hbErr
	}
	return s.hbResp.ToProto(), nil
}

func TestDataNodeServer_heartbeat(t *testing.T) {
	mockNodeManager := &cluster.MockNodeManager{}
	server := &DataNodeServer{
		Config: DataNodeConfig{
			Info: common.DataNodeInfo{ID: "node1", Status: common.NodeHealthy},
		},
		NodeManager: mockNodeManager,
		logger:      logging.NewTestLogger(slog.LevelWarn),
	}

	tests := []struct {
		name              string
		expectedError     error
		ctx               context.Context
		heartbeatResponse coordinator.HeartbeatResponse
	}{
		{
			name:          "success",
			expectedError: nil,
			ctx:           context.Background(),
			heartbeatResponse: coordinator.HeartbeatResponse{
				Success:            true,
				RequiresFullResync: false,
				FromVersion:        1,
				ToVersion:          2,
				Updates: []cluster.NodeUpdate{
					{
						Node: &common.DataNodeInfo{
							ID:       "node2",
							Status:   common.NodeHealthy,
							LastSeen: time.Now(),
						},
						Version:   2,
						Type:      cluster.NODE_ADDED,
						Timestamp: time.Now(),
					},
				},
			},
		},
		{
			name:          "error: heartbeat failed",
			expectedError: errors.New("heartbeat failed"),
			ctx:           context.Background(),
			heartbeatResponse: coordinator.HeartbeatResponse{
				Success: false,
				Message: "heartbeat failed",
			},
		},
		{
			name:          "error: requires full resync",
			expectedError: ErrRequireResync,
			ctx:           context.Background(),
			heartbeatResponse: coordinator.HeartbeatResponse{
				Success:            true,
				RequiresFullResync: true,
				Message:            "requires full resync",
			},
		},
		{
			name:          "error: success but no updates sent",
			expectedError: errors.New("no updates sent"),
			ctx:           context.Background(),
			heartbeatResponse: coordinator.HeartbeatResponse{
				Success:            true,
				RequiresFullResync: false,
				FromVersion:        1,
				ToVersion:          2,
				Updates:            []cluster.NodeUpdate{},
			},
		},
		{
			name:          "error: same version",
			expectedError: errors.New("no updates sent"),
			ctx:           context.Background(),
			heartbeatResponse: coordinator.HeartbeatResponse{
				Success:            true,
				RequiresFullResync: false,
				FromVersion:        1,
				ToVersion:          1,
				Updates: []cluster.NodeUpdate{
					{
						Node: &common.DataNodeInfo{
							ID:       "node2",
							Status:   common.NodeHealthy,
							LastSeen: time.Now(),
						},
						Version:   2,
						Type:      cluster.NODE_ADDED,
						Timestamp: time.Now(),
					},
				},
			},
		},
		{
			name:          "error: context canceled",
			expectedError: context.Canceled,
			ctx: func() context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx
			}(),
			heartbeatResponse: coordinator.HeartbeatResponse{
				Success: false,
				Message: "context canceled",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			coordinatorClient, cleanup := NewTestCoordinatorClientWithStubServer(t, &stubCoordinatorHeartbeatServer{hbResp: tt.heartbeatResponse, hbErr: tt.expectedError})
			defer cleanup()

			// Setup the node manager mock on each test run
			mockNodeManager.On("GetCurrentVersion").Return(int64(1)).Once()
			if tt.expectedError == nil {
				// If no error, we expect the node manager to apply the history
				mockNodeManager.On("ApplyHistory", mock.AnythingOfType("[]cluster.NodeUpdate")).Once()
			}

			// Coordinator client is injected into the heartbeat function
			err := server.heartbeat(tt.ctx, coordinatorClient)
			if err != nil && tt.expectedError == nil {
				t.Errorf("heartbeat() error = %v, wantErr %v", err, tt.expectedError)
			}

			mockNodeManager.AssertExpectations(t)
		})
	}
}
