package datanode_services

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/mochivi/distributed-file-system/internal/clients"
	"github.com/mochivi/distributed-file-system/internal/cluster/state"
	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/pkg/logging"
	"github.com/mochivi/distributed-file-system/pkg/proto"
	"github.com/mochivi/distributed-file-system/pkg/testutils"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc"
)

type stubCoordinatorRegisterServer struct {
	proto.UnimplementedCoordinatorServiceServer
	registerResp common.RegisterDataNodeResponse
	registerErr  error
}

func (s *stubCoordinatorRegisterServer) RegisterDataNode(ctx context.Context, req *proto.RegisterDataNodeRequest) (*proto.RegisterDataNodeResponse, error) {
	if s.registerErr != nil {
		return nil, s.registerErr
	}
	return s.registerResp.ToProto(), nil
}

func TestRegisterService_RegisterWithCoordinator(t *testing.T) {
	nodeInfo := &common.DataNodeInfo{
		ID:       "node1",
		Status:   common.NodeHealthy,
		LastSeen: time.Now(),
	}

	// Create a coordinator finder
	coordinatorNodeInfo := &common.DataNodeInfo{
		ID:       "coordinator",
		Status:   common.NodeHealthy,
		LastSeen: time.Now(),
	}
	coordinatorFinder := state.NewCoordinatorFinder()
	coordinatorFinder.AddCoordinator(coordinatorNodeInfo)

	tests := []struct {
		name                    string
		setupCtx                func(t *testing.T) context.Context
		setupMocks              func(mockClusterStateManager *state.MockClusterStateManager)
		coordinatorRegisterResp common.RegisterDataNodeResponse
		coordinatorRegisterErr  error
		expectedError           error
	}{
		{
			name: "success",
			setupCtx: func(t *testing.T) context.Context {
				return context.Background()
			},
			setupMocks: func(mockClusterStateManager *state.MockClusterStateManager) {
				mockClusterStateManager.On("InitializeNodes", mock.AnythingOfType("[]*common.DataNodeInfo"), mock.AnythingOfType("int64")).Return(nil)
			},
			coordinatorRegisterResp: common.RegisterDataNodeResponse{
				Success: true,
			},
			coordinatorRegisterErr: nil, // Error returned by coordinator on register attempt
			expectedError:          nil, // Error returned by the register service RegisterWithCoordinator function
		},
		{
			name: "error: context cancelled",
			setupCtx: func(t *testing.T) context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx
			},
			setupMocks: func(mockClusterStateManager *state.MockClusterStateManager) {},
			coordinatorRegisterResp: common.RegisterDataNodeResponse{
				Success: true,
			},
			coordinatorRegisterErr: nil,
			expectedError:          context.Canceled,
		},
		{
			name: "error: coordinator returned unsuccessful response",
			setupCtx: func(t *testing.T) context.Context {
				return context.Background()
			},
			setupMocks: func(mockClusterStateManager *state.MockClusterStateManager) {},
			coordinatorRegisterResp: common.RegisterDataNodeResponse{
				Success: false,
			},
			coordinatorRegisterErr: nil,
			expectedError:          errors.New("coordinator returned unsuccessful response"),
		},
		{
			name: "error: node list was empty but version was not 0",
			setupCtx: func(t *testing.T) context.Context {
				return context.Background()
			},
			setupMocks: func(mockClusterStateManager *state.MockClusterStateManager) {},
			coordinatorRegisterResp: common.RegisterDataNodeResponse{
				Success:        true,
				FullNodeList:   []*common.DataNodeInfo{},
				CurrentVersion: 1,
			},
			coordinatorRegisterErr: nil,
			expectedError:          errors.New("node list was empty but version was not 0"),
		},
		{
			name: "error: coordinator returned error",
			setupCtx: func(t *testing.T) context.Context {
				return context.Background()
			},
			setupMocks:              func(mockClusterStateManager *state.MockClusterStateManager) {},
			coordinatorRegisterResp: common.RegisterDataNodeResponse{},
			coordinatorRegisterErr:  errors.New("coordinator returned error"),
			expectedError:           errors.New("coordinator returned error"),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := tc.setupCtx(t)
			mockClusterStateManager := &state.MockClusterStateManager{}
			tc.setupMocks(mockClusterStateManager)

			// Setup coordinator finder to open a connection to a coordinator with a stub server
			coordinatorFinder.SetClientConnectionFunc(func(node *common.DataNodeInfo, opts ...grpc.DialOption) (clients.ICoordinatorClient, error) {
				coordinatorClient, _ := testutils.NewTestCoordinatorClientWithStubServer(t, &stubCoordinatorRegisterServer{
					registerResp: tc.coordinatorRegisterResp,
					registerErr:  tc.coordinatorRegisterErr,
				})
				return coordinatorClient, nil
			})

			registerService := NewRegisterService(logging.NewTestLogger(slog.LevelError))

			err := registerService.RegisterWithCoordinator(ctx, nodeInfo, mockClusterStateManager, coordinatorFinder)

			if err != nil && tc.expectedError == nil {
				if errors.Is(err, context.Canceled) && tc.name != "error: context canceled" {
					return
				}
				t.Errorf("controller.Run() error = %v, wantErr %v", err, tc.expectedError)
			} else if err == nil && tc.expectedError != nil {
				t.Errorf("controller.Run() error = %v, wantErr %v", err, tc.expectedError)
			}

			mockClusterStateManager.AssertExpectations(t)
		})
	}
}
