package datanode_controllers

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/mochivi/distributed-file-system/internal/clients"
	"github.com/mochivi/distributed-file-system/internal/cluster/state"
	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/internal/config"
	"github.com/mochivi/distributed-file-system/pkg/logging"
	"github.com/mochivi/distributed-file-system/pkg/proto"
	"github.com/mochivi/distributed-file-system/pkg/testutils"
	"github.com/stretchr/testify/mock"
)

// Heartbeat stub server
type stubCoordinatorHeartbeatServer struct {
	proto.UnimplementedCoordinatorServiceServer
	hbResp common.HeartbeatResponse // pre-programmed response
	hbErr  error
}

func (s *stubCoordinatorHeartbeatServer) DataNodeHeartbeat(ctx context.Context, req *proto.HeartbeatRequest) (*proto.HeartbeatResponse, error) {
	if s.hbErr != nil {
		return nil, s.hbErr
	}
	return s.hbResp.ToProto(), nil
}

func TestHeartbeatController_RunLoop(t *testing.T) {
	nodeInfo := &common.DataNodeInfo{
		ID:       "node1",
		Status:   common.NodeHealthy,
		LastSeen: time.Now(),
	}

	// Create a coordinator finder - the returned client is not actually used, so this just has to exist while not doing anything
	coordinatorNodeInfo := &common.DataNodeInfo{
		ID:       "coordinator",
		Status:   common.NodeHealthy,
		LastSeen: time.Now(),
	}
	coordinatorFinder := state.NewCoordinatorFinder()
	coordinatorFinder.AddCoordinator(coordinatorNodeInfo)

	tests := []struct {
		name              string
		setupCtx          func(t *testing.T) context.Context
		setupMocks        func(mockClusterStateManager *state.MockClusterStateManager)
		heartbeatResponse common.HeartbeatResponse
		heartbeatError    error // error returned by the inner heartbeat function
		expectedError     error // error returned by the controller.Run function
	}{
		{
			name: "success",
			setupCtx: func(t *testing.T) context.Context {
				return context.Background()
			},
			setupMocks: func(mockClusterStateManager *state.MockClusterStateManager) {
				mockClusterStateManager.On("GetCurrentVersion").Return(int64(1))
				mockClusterStateManager.On("ApplyUpdates", mock.AnythingOfType("[]common.NodeUpdate")).Return(nil)
			},
			heartbeatResponse: common.HeartbeatResponse{
				Success:            true,
				RequiresFullResync: false,
				FromVersion:        1,
				ToVersion:          2,
			},
			heartbeatError: nil,
			expectedError:  nil,
		},
		{
			name: "error: context cancelled",
			setupCtx: func(t *testing.T) context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx
			},
			setupMocks: func(mockClusterStateManager *state.MockClusterStateManager) {},
			heartbeatResponse: common.HeartbeatResponse{
				Success: true,
			},
			heartbeatError: nil,
			expectedError:  context.Canceled,
		},
		{
			name: "error: heartbeat failed",
			setupCtx: func(t *testing.T) context.Context {
				return context.Background()
			},
			setupMocks: func(mockClusterStateManager *state.MockClusterStateManager) {
				mockClusterStateManager.On("GetCurrentVersion").Return(int64(1))
			},
			heartbeatResponse: common.HeartbeatResponse{
				Success: false,
				Message: "heartbeat failed",
			},
			heartbeatError: errors.New("heartbeat failed"),
			expectedError:  context.Canceled,
		},
		{
			name: "error: heartbeat failed with no updates",
			setupCtx: func(t *testing.T) context.Context {
				return context.Background()
			},
			setupMocks: func(mockClusterStateManager *state.MockClusterStateManager) {
				mockClusterStateManager.On("GetCurrentVersion").Return(int64(1))
			},
			heartbeatResponse: common.HeartbeatResponse{
				Success:            true,
				RequiresFullResync: false,
				FromVersion:        1,
				ToVersion:          2,
				Updates:            []common.NodeUpdate{},
			},
			heartbeatError: ErrNoUpdates,
			expectedError:  nil,
		},
		{
			name: "error: heartbeat failed with same version",
			setupCtx: func(t *testing.T) context.Context {
				return context.Background()
			},
			setupMocks: func(mockClusterStateManager *state.MockClusterStateManager) {
				mockClusterStateManager.On("GetCurrentVersion").Return(int64(1))
			},
			heartbeatResponse: common.HeartbeatResponse{
				Success:            true,
				RequiresFullResync: false,
				FromVersion:        1,
				ToVersion:          1,
				Updates:            []common.NodeUpdate{},
			},
			heartbeatError: ErrSameVersion,
			expectedError:  nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := tc.setupCtx(t)
			controller := NewHeartbeatController(ctx, &config.HeartbeatControllerConfig{
				Interval: 100 * time.Millisecond,
				Timeout:  10 * time.Second,
			}, logging.NewTestLogger(slog.LevelError))

			// Mock heartbeat function
			controller.heartbeatFunc = func(ctx context.Context, req common.HeartbeatRequest, client clients.ICoordinatorClient) ([]common.NodeUpdate, error) {
				return tc.heartbeatResponse.Updates, tc.heartbeatError
			}

			// Mock cluster state manager
			mockClusterStateManager := &state.MockClusterStateManager{}
			tc.setupMocks(mockClusterStateManager)

			// Automate cancelling the loop
			go func() {
				time.Sleep(1 * time.Second)
				controller.Cancel(tc.expectedError)
			}()

			// Run the loop
			err := controller.Run(nodeInfo, mockClusterStateManager, coordinatorFinder)

			if err != nil && tc.expectedError == nil {
				if errors.Is(err, context.Canceled) && tc.name != "error: context canceled" {
					return
				}
				t.Errorf("controller.Run() error = %v, wantErr %v", err, tc.expectedError)
			} else if err == nil && tc.expectedError != nil {
				if errors.Is(err, context.Canceled) {
					return
				}
				t.Errorf("controller.Run() error = %v, wantErr %v", err, tc.expectedError)
			}

			mockClusterStateManager.AssertExpectations(t)
		})
	}
}

func TestHeartbeatController_heartbeat(t *testing.T) {

	heartbeatRequest := common.HeartbeatRequest{
		NodeID:          "node1",
		Status:          common.HealthStatus{Status: common.NodeHealthy, LastSeen: time.Now()},
		LastSeenVersion: 1,
	}

	tests := []struct {
		name              string
		setupCtx          func(t *testing.T) (context.Context, context.CancelCauseFunc)
		heartbeatRequest  common.HeartbeatRequest
		heartbeatResponse common.HeartbeatResponse
		expectedError     error
	}{
		{
			name: "success",
			setupCtx: func(t *testing.T) (context.Context, context.CancelCauseFunc) {
				ctx, cancel := context.WithCancelCause(context.Background())
				return ctx, cancel
			},
			heartbeatRequest: heartbeatRequest,
			heartbeatResponse: common.HeartbeatResponse{
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
			expectedError: nil,
		},
		{
			name: "error: heartbeat failed with message",
			setupCtx: func(t *testing.T) (context.Context, context.CancelCauseFunc) {
				ctx, cancel := context.WithCancelCause(context.Background())
				return ctx, cancel
			},
			heartbeatRequest: heartbeatRequest,
			heartbeatResponse: common.HeartbeatResponse{
				Success: false,
				Message: "heartbeat failed",
			},
			expectedError: errors.New("heartbeat failed"),
		},
		{
			name: "error: unsuccessful heartbeat",
			setupCtx: func(t *testing.T) (context.Context, context.CancelCauseFunc) {
				ctx, cancel := context.WithCancelCause(context.Background())
				return ctx, cancel
			},
			heartbeatRequest: heartbeatRequest,
			heartbeatResponse: common.HeartbeatResponse{
				Success: false,
			},
			expectedError: errors.New("heartbeat failed"),
		},
		{
			name: "error: context canceled",
			setupCtx: func(t *testing.T) (context.Context, context.CancelCauseFunc) {
				ctx, cancel := context.WithCancelCause(context.Background())
				cancel(context.Canceled)
				return ctx, cancel
			},
			heartbeatRequest:  heartbeatRequest,
			heartbeatResponse: common.HeartbeatResponse{},
			expectedError:     context.Canceled,
		},
		{
			name: "error: requires full resync",
			setupCtx: func(t *testing.T) (context.Context, context.CancelCauseFunc) {
				ctx, cancel := context.WithCancelCause(context.Background())
				return ctx, cancel
			},
			heartbeatResponse: common.HeartbeatResponse{
				Success:            true,
				RequiresFullResync: true,
				Message:            "requires full resync",
			},
			expectedError: ErrRequireResync,
		},
		{
			name: "error: success but no updates sent",
			setupCtx: func(t *testing.T) (context.Context, context.CancelCauseFunc) {
				ctx, cancel := context.WithCancelCause(context.Background())
				return ctx, cancel
			},
			heartbeatRequest: heartbeatRequest,
			heartbeatResponse: common.HeartbeatResponse{
				Success:            true,
				RequiresFullResync: false,
				FromVersion:        1,
				ToVersion:          2,
				Updates:            []common.NodeUpdate{},
			},
			expectedError: ErrNoUpdates,
		},
		{
			name: "error: same version",
			setupCtx: func(t *testing.T) (context.Context, context.CancelCauseFunc) {
				ctx, cancel := context.WithCancelCause(context.Background())
				return ctx, cancel
			},
			heartbeatRequest: heartbeatRequest,
			heartbeatResponse: common.HeartbeatResponse{
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
			expectedError: ErrSameVersion,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {

			ctx, cancel := tc.setupCtx(t)
			controller := &HeartbeatController{
				ctx:    ctx,
				cancel: cancel,
				config: &config.HeartbeatControllerConfig{
					Interval: 1 * time.Second,
					Timeout:  10 * time.Second,
				},
				logger: logging.NewTestLogger(slog.LevelError),
			}

			coordinatorClient, cleanup := testutils.NewTestCoordinatorClientWithStubServer(t, &stubCoordinatorHeartbeatServer{hbResp: tc.heartbeatResponse, hbErr: tc.expectedError})
			defer cleanup()

			// Coordinator client is injected into the heartbeat function
			_, err := controller.heartbeat(ctx, tc.heartbeatRequest, coordinatorClient)
			if err != nil && tc.expectedError == nil {
				t.Errorf("heartbeat() error = %v, wantErr %v", err, tc.expectedError)
			}

			if err == nil && tc.expectedError != nil {
				t.Errorf("heartbeat() error = %v, wantErr %v", err, tc.expectedError)
			}
		})
	}
}
