package cluster

import (
	"context"
	"errors"
	"sync"
	"testing"

	datanode_controllers "github.com/mochivi/distributed-file-system/internal/cluster/datanode/controllers"
	datanode_services "github.com/mochivi/distributed-file-system/internal/cluster/datanode/services"
	"github.com/mochivi/distributed-file-system/internal/cluster/state"
	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/stretchr/testify/assert"
)

// agentMocks is a helper struct to hold all mocks for the agent tests.
type agentMocks struct {
	coordinator *state.MockCoordinatorFinder
	heartbeat   *datanode_controllers.MockHeartbeatController
	register    *datanode_services.MockRegisterService
	csm         *state.MockClusterStateManager
}

// newAgentMocks initializes all mocks.
func newAgentMocks() agentMocks {
	return agentMocks{
		coordinator: state.NewMockCoordinatorFinder(),
		heartbeat:   datanode_controllers.NewMockHeartbeatController(),
		register:    datanode_services.NewMockRegisterService(),
		csm:         state.NewMockClusterStateManager(),
	}
}

// AssertExpectations asserts that all mock expectations were met.
func (m *agentMocks) AssertExpectations(t *testing.T) {
	m.coordinator.AssertExpectations(t)
	m.heartbeat.AssertExpectations(t)
	m.register.AssertExpectations(t)
	m.csm.AssertExpectations(t)
}

func TestNodeAgent_Run(t *testing.T) {
	nodeInfo := &common.DataNodeInfo{ID: "test-node"}

	testCases := []struct {
		name        string
		setupMocks  func(agent *NodeAgent, mocks *agentMocks)
		expectError bool
	}{
		{
			name: "success: bootstrap and launch services",
			setupMocks: func(agent *NodeAgent, mocks *agentMocks) {
				mocks.coordinator.On("BootstrapCoordinator").Return(nil).Once()
				mocks.heartbeat.On("Run", agent.info, agent.clusterStateManager, agent.services.Coordinator).Return(nil).Once()
				mocks.register.On("RegisterWithCoordinator", agent.ctx, agent.info, agent.clusterStateManager, agent.services.Coordinator).Return(nil).Once()
			},
			expectError: false,
		},
		{
			name: "error: bootstrap fails",
			setupMocks: func(agent *NodeAgent, mocks *agentMocks) {
				mocks.coordinator.On("BootstrapCoordinator").Return(errors.New("bootstrap failed")).Once()
			},
			expectError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Setup
			mocks := newAgentMocks()
			agent := &NodeAgent{
				wg:                  sync.WaitGroup{},
				ctx:                 context.Background(),
				info:                nodeInfo,
				clusterStateManager: mocks.csm,
				controllers: datanode_controllers.NodeAgentControllers{
					Heartbeat: mocks.heartbeat,
				},
				services: datanode_services.NodeAgentServices{
					Coordinator: mocks.coordinator,
					Register:    mocks.register,
				},
			}

			tc.setupMocks(agent, &mocks)

			// Execute
			err := agent.Run()

			// Assert
			if tc.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			mocks.AssertExpectations(t)
		})
	}
}
