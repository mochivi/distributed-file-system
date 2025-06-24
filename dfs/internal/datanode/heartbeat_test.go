package datanode

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/internal/coordinator"
	"github.com/mochivi/distributed-file-system/pkg/logging"
	"github.com/stretchr/testify/mock"
)

func TestHeartbeat(t *testing.T) {
	mockNodeManager := &common.MockNodeManager{}
	mockCoordinatorClient := &coordinator.MockCoordinatorClient{}

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
				Updates: []common.NodeUpdate{
					{
						Node: &common.DataNodeInfo{
							ID:       "node2",
							Status:   common.NodeHealthy,
							LastSeen: time.Now(),
						},
						Version:   2,
						Type:      common.NODE_ADDED,
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
				Updates:            []common.NodeUpdate{},
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
				Updates: []common.NodeUpdate{
					{
						Node: &common.DataNodeInfo{
							ID:       "node2",
							Status:   common.NodeHealthy,
							LastSeen: time.Now(),
						},
						Version:   2,
						Type:      common.NODE_ADDED,
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
			mockCoordinatorClient.On("Node").Return(&common.DataNodeInfo{ID: "coordinator"}).Once()
			mockNodeManager.On("GetCurrentVersion").Return(int64(1)).Once()
			mockCoordinatorClient.On("DataNodeHeartbeat", tt.ctx, mock.AnythingOfType("coordinator.HeartbeatRequest")).Return(tt.heartbeatResponse, tt.expectedError).Once()

			if tt.expectedError == nil {
				mockNodeManager.On("ApplyHistory", mock.AnythingOfType("[]common.NodeUpdate")).Once()
			}

			err := server.heartbeat(tt.ctx, mockCoordinatorClient)
			if err != nil && tt.expectedError == nil {
				t.Errorf("heartbeat() error = %v, wantErr %v", err, tt.expectedError)
			}

			mockNodeManager.AssertExpectations(t)
			mockCoordinatorClient.AssertExpectations(t)
		})
	}
}
