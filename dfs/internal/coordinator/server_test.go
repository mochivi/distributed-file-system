package coordinator

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/mochivi/distributed-file-system/internal/cluster/state"
	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/internal/config"
	"github.com/mochivi/distributed-file-system/internal/storage/chunk"
	"github.com/mochivi/distributed-file-system/internal/storage/metadata"
	"github.com/mochivi/distributed-file-system/pkg/logging"
	"github.com/mochivi/distributed-file-system/pkg/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type serverMocks struct {
	clusterStateHistoryManager *state.MockClusterStateHistoryManager
	nodeSelector               *state.MockNodeSelector
	metaStore                  *metadata.MockMetadataStore
	metadataManager            *MockMetadataSessionManager
}

func (m *serverMocks) assertExpectations(t *testing.T) {
	m.clusterStateHistoryManager.AssertExpectations(t)
	m.nodeSelector.AssertExpectations(t)
	m.metaStore.AssertExpectations(t)
	m.metadataManager.AssertExpectations(t)
}

func TestCoordinator_UploadFile(t *testing.T) {
	cfg := config.CoordinatorConfig{
		ChunkSize: 1 * 1024 * 1024, // 1MB
		Metadata: config.MetadataConfig{
			CommitTimeout: 5 * time.Second,
		},
	}
	logger := logging.NewTestLogger(slog.LevelError, true)

	testCases := []struct {
		name         string
		setupMocks   func(*serverMocks)
		req          *proto.UploadRequest
		expectedResp func(sessionID string) *proto.UploadResponse
		expectErr    bool
	}{
		{
			name: "success: 1 chunk",
			setupMocks: func(mocks *serverMocks) {
				mocks.nodeSelector.On("SelectBestNodes", 1, &common.NodeInfo{ID: "coordinator"}).Return([]*common.NodeInfo{
					{ID: "node1", Status: common.NodeHealthy, Host: "127.0.0.1", Port: 8081},
				}, nil)
				mocks.metadataManager.On("trackUpload", mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
			},
			req: &proto.UploadRequest{
				Path:      "test.txt",
				ChunkSize: 1 * 1024 * 1024,
				Size:      (1 * 1024 * 1024) - 1, // 1MB - 1B
				Checksum:  "test-checksum",
			},
			expectedResp: func(sessionID string) *proto.UploadResponse {
				return &proto.UploadResponse{
					SessionId: sessionID,
					ChunkIds:  []string{chunk.FormatChunkID("test.txt", 0)},
					Nodes:     []*proto.NodeInfo{{Id: "node1", Status: proto.NodeStatus_NODE_HEALTHY, IpAddress: "127.0.0.1", Port: 8081}},
				}
			},
			expectErr: false,
		},
		{
			name: "success: 2 chunks",
			setupMocks: func(mocks *serverMocks) {
				mocks.nodeSelector.On("SelectBestNodes", 2, &common.NodeInfo{ID: "coordinator"}).Return([]*common.NodeInfo{
					{ID: "node1", Status: common.NodeHealthy, Host: "127.0.0.1", Port: 8081},
				}, nil)
				mocks.metadataManager.On("trackUpload", mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
			},
			req: &proto.UploadRequest{
				Path:      "test.txt",
				ChunkSize: 1 * 1024 * 1024,
				Size:      (2 * 1024 * 1024) - 1, // 2MB - 1B
				Checksum:  "test-checksum",
			},
			expectedResp: func(sessionID string) *proto.UploadResponse {
				return &proto.UploadResponse{
					SessionId: sessionID,
					ChunkIds:  []string{chunk.FormatChunkID("test.txt", 0), chunk.FormatChunkID("test.txt", 1)},
					Nodes:     []*proto.NodeInfo{{Id: "node1", Status: proto.NodeStatus_NODE_HEALTHY, IpAddress: "127.0.0.1", Port: 8081}},
				}
			},
			expectErr: false,
		},
		{
			name: "error: node selector not ok",
			setupMocks: func(mocks *serverMocks) {
				mocks.nodeSelector.On("SelectBestNodes", 1, &common.NodeInfo{ID: "coordinator"}).Return(nil, state.ErrNoAvailableNodes)
			},
			req: &proto.UploadRequest{
				Path:      "test.txt",
				ChunkSize: 1 * 1024 * 1024,
				Size:      (1 * 1024 * 1024) - 1, // 1MB - 1B
				Checksum:  "test-checksum",
			},
			expectedResp: nil,
			expectErr:    true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mocks := &serverMocks{
				nodeSelector:               new(state.MockNodeSelector),
				metaStore:                  new(metadata.MockMetadataStore),
				clusterStateHistoryManager: new(state.MockClusterStateHistoryManager),
				metadataManager:            new(MockMetadataSessionManager),
			}
			tc.setupMocks(mocks)
			container := NewContainer(mocks.metaStore, mocks.metadataManager, mocks.clusterStateHistoryManager, mocks.nodeSelector)
			coordinator := NewCoordinator(&cfg, container, logger)

			resp, err := coordinator.UploadFile(context.Background(), tc.req)
			time.Sleep(1 * time.Second) // wait for metadataManager goroutine to start and finish, this is a hack to avoid race conditions

			if tc.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				expected := tc.expectedResp(resp.SessionId)
				assert.Equal(t, expected.ChunkIds, resp.ChunkIds)
				assert.Equal(t, expected.Nodes, resp.Nodes)
				assert.NotEmpty(t, resp.SessionId)
			}

			mocks.assertExpectations(t)
		})
	}
}

func TestCoordinator_DownloadFile(t *testing.T) {
	cfg := config.DefaultCoordinatorConfig()
	logger := logging.NewTestLogger(slog.LevelError, true)

	testCases := []struct {
		name         string
		setupMocks   func(*serverMocks)
		req          *proto.DownloadRequest
		expectedResp *proto.DownloadResponse
		expectErr    bool
	}{
		{
			name: "success",
			setupMocks: func(mocks *serverMocks) {
				fileInfo := &common.FileInfo{
					Path:       "test.txt",
					Size:       1 * 1024 * 1024,
					ChunkCount: 1,
					Chunks: []common.ChunkInfo{
						{Header: common.ChunkHeader{ID: "chunk1"}, Replicas: []*common.NodeInfo{{ID: "node1"}}},
					},
					CreatedAt: time.Unix(0, 0),
					Checksum:  "test-checksum",
				}
				nodes := []*common.NodeInfo{{ID: "node1", Host: "localhost", Port: 9001, Status: common.NodeHealthy}}
				mocks.metaStore.On("GetFile", mock.Anything, "test.txt").Return(fileInfo, nil).Once()
				mocks.clusterStateHistoryManager.On("GetAvailableNodesForChunk", mock.Anything).Return(nodes, nil).Once()
			},
			req: &proto.DownloadRequest{Path: "test.txt"},
			expectedResp: &proto.DownloadResponse{
				FileInfo: (&common.FileInfo{
					Path:       "test.txt",
					Size:       1 * 1024 * 1024,
					ChunkCount: 1,
					Chunks:     []common.ChunkInfo{{Header: common.ChunkHeader{ID: "chunk1"}, Replicas: []*common.NodeInfo{{ID: "node1"}}}},
					CreatedAt:  time.Unix(0, 0),
					Checksum:   "test-checksum",
				}).ToProto(),
				ChunkLocations: []*proto.ChunkLocation{
					{ChunkId: "chunk1", Nodes: []*proto.NodeInfo{{Id: "node1", IpAddress: "localhost", Port: 9001, Status: proto.NodeStatus_NODE_HEALTHY}}},
				},
			},
			expectErr: false,
		},
		{
			name: "success: 2 chunks",
			setupMocks: func(mocks *serverMocks) {
				fileInfo := &common.FileInfo{
					Path:       "test.txt",
					Size:       2 * 1024 * 1024,
					ChunkCount: 2,
					Chunks: []common.ChunkInfo{
						{Header: common.ChunkHeader{ID: "chunk1"}, Replicas: []*common.NodeInfo{{ID: "node1"}}},
						{Header: common.ChunkHeader{ID: "chunk2"}, Replicas: []*common.NodeInfo{{ID: "node1"}}},
					},
					CreatedAt: time.Unix(0, 0),
				}
				nodes := []*common.NodeInfo{{ID: "node1", Host: "localhost", Port: 9001, Status: common.NodeHealthy}}
				mocks.metaStore.On("GetFile", mock.Anything, "test.txt").Return(fileInfo, nil).Once()
				mocks.clusterStateHistoryManager.On("GetAvailableNodesForChunk", mock.Anything).Return(nodes, nil)
			},
			req: &proto.DownloadRequest{Path: "test.txt"},
			expectedResp: &proto.DownloadResponse{
				FileInfo: (&common.FileInfo{
					Path:       "test.txt",
					Size:       2 * 1024 * 1024,
					ChunkCount: 2,
					Chunks: []common.ChunkInfo{
						{Header: common.ChunkHeader{ID: "chunk1"}, Replicas: []*common.NodeInfo{{ID: "node1"}}},
						{Header: common.ChunkHeader{ID: "chunk2"}, Replicas: []*common.NodeInfo{{ID: "node1"}}},
					},
					CreatedAt: time.Unix(0, 0),
				}).ToProto(),
				ChunkLocations: []*proto.ChunkLocation{
					{ChunkId: "chunk1", Nodes: []*proto.NodeInfo{{Id: "node1", IpAddress: "localhost", Port: 9001, Status: proto.NodeStatus_NODE_HEALTHY}}},
					{ChunkId: "chunk2", Nodes: []*proto.NodeInfo{{Id: "node1", IpAddress: "localhost", Port: 9001, Status: proto.NodeStatus_NODE_HEALTHY}}},
				},
			},
			expectErr: false,
		},
		{
			name: "error: file not found",
			setupMocks: func(mocks *serverMocks) {
				mocks.metaStore.On("GetFile", mock.Anything, "test.txt").Return(nil, errors.New("file not found")).Once()
			},
			req:          &proto.DownloadRequest{Path: "test.txt"},
			expectedResp: nil,
			expectErr:    true,
		},
		{
			name: "error: no available nodes",
			setupMocks: func(mocks *serverMocks) {
				fileInfo := &common.FileInfo{
					Path:       "test.txt",
					Size:       1 * 1024 * 1024,
					ChunkCount: 1,
					Chunks:     []common.ChunkInfo{{Header: common.ChunkHeader{ID: "chunk1"}, Replicas: []*common.NodeInfo{{ID: "node1"}}}},
					CreatedAt:  time.Unix(0, 0),
				}
				mocks.metaStore.On("GetFile", mock.Anything, "test.txt").Return(fileInfo, nil).Once()
				mocks.clusterStateHistoryManager.On("GetAvailableNodesForChunk", mock.Anything).Return(nil, state.ErrNoAvailableNodes).Once()
			},
			req:          &proto.DownloadRequest{Path: "test.txt"},
			expectedResp: nil,
			expectErr:    true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mocks := &serverMocks{
				clusterStateHistoryManager: new(state.MockClusterStateHistoryManager),
				metaStore:                  new(metadata.MockMetadataStore),
				metadataManager:            new(MockMetadataSessionManager),
				nodeSelector:               new(state.MockNodeSelector),
			}
			tc.setupMocks(mocks)
			container := NewContainer(mocks.metaStore, mocks.metadataManager, mocks.clusterStateHistoryManager, mocks.nodeSelector)
			coordinator := NewCoordinator(&cfg, container, logger)

			resp, err := coordinator.DownloadFile(context.Background(), tc.req)

			if tc.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expectedResp.FileInfo, resp.FileInfo)
				for i, expected := range tc.expectedResp.ChunkLocations {
					assert.Equal(t, expected.ChunkId, resp.ChunkLocations[i].ChunkId)
					for j, node := range expected.Nodes {
						assert.Equal(t, node.Id, resp.ChunkLocations[i].Nodes[j].Id)
						assert.Equal(t, node.IpAddress, resp.ChunkLocations[i].Nodes[j].IpAddress)
						assert.Equal(t, node.Port, resp.ChunkLocations[i].Nodes[j].Port)
					}
				}
				assert.NotEmpty(t, resp.SessionId)
			}

			mocks.metaStore.AssertExpectations(t)
			mocks.clusterStateHistoryManager.AssertExpectations(t)
		})
	}
}

func TestCoordinator_DeleteFile(t *testing.T) {
	cfg := config.DefaultCoordinatorConfig()
	logger := logging.NewTestLogger(slog.LevelError, true)

	testCases := []struct {
		name         string
		setupMocks   func(*serverMocks)
		req          *proto.DeleteRequest
		expectedResp *proto.DeleteResponse
		expectErr    bool
	}{
		{
			name: "success",
			setupMocks: func(mocks *serverMocks) {
				mocks.metaStore.On("DeleteFile", mock.Anything, "test.txt").Return(nil).Once()
			},
			req:          &proto.DeleteRequest{Path: "test.txt"},
			expectedResp: &proto.DeleteResponse{Success: true},
			expectErr:    false,
		},
		{
			name: "error: delete fails",
			setupMocks: func(mocks *serverMocks) {
				mocks.metaStore.On("DeleteFile", mock.Anything, "test.txt").Return(assert.AnError).Once()
			},
			req:          &proto.DeleteRequest{Path: "test.txt"},
			expectedResp: nil,
			expectErr:    true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mocks := &serverMocks{
				metaStore: new(metadata.MockMetadataStore),
			}
			tc.setupMocks(mocks)
			container := NewContainer(mocks.metaStore, mocks.metadataManager, mocks.clusterStateHistoryManager, mocks.nodeSelector)
			coordinator := NewCoordinator(&cfg, container, logger)

			resp, err := coordinator.DeleteFile(context.Background(), tc.req)

			if tc.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expectedResp, resp)
			}
			mocks.metaStore.AssertExpectations(t)
		})
	}
}

func TestCoordinator_ConfirmUpload(t *testing.T) {
	cfg := config.DefaultCoordinatorConfig()
	logger := logging.NewTestLogger(slog.LevelError, true)

	testCases := []struct {
		name         string
		setupMocks   func(*serverMocks)
		req          *proto.ConfirmUploadRequest
		expectedResp *proto.ConfirmUploadResponse
		expectErr    bool
	}{
		{
			name: "success",
			setupMocks: func(mocks *serverMocks) {
				mocks.metadataManager.On("commit", mock.Anything, "test-session", mock.Anything, mocks.metaStore).Return(nil).Once()
			},
			req: &proto.ConfirmUploadRequest{
				SessionId: "test-session",
			},
			expectedResp: &proto.ConfirmUploadResponse{Success: true},
			expectErr:    false,
		},
		{
			name: "error: commit fails",
			setupMocks: func(mocks *serverMocks) {
				mocks.metadataManager.On("commit", mock.Anything, "test-session-fail", mock.Anything, mocks.metaStore).Return(assert.AnError).Once()
			},
			req: &proto.ConfirmUploadRequest{
				SessionId: "test-session-fail",
			},
			expectedResp: &proto.ConfirmUploadResponse{Success: false, Message: "Failed to commit metadata"},
			expectErr:    false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mocks := &serverMocks{
				metaStore:       new(metadata.MockMetadataStore),
				metadataManager: new(MockMetadataSessionManager),
			}
			tc.setupMocks(mocks)
			container := NewContainer(mocks.metaStore, mocks.metadataManager, mocks.clusterStateHistoryManager, mocks.nodeSelector)
			coordinator := NewCoordinator(&cfg, container, logger)
			resp, err := coordinator.ConfirmUpload(context.Background(), tc.req)

			if tc.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expectedResp, resp)
			}
			mocks.metaStore.AssertExpectations(t)
			mocks.metadataManager.AssertExpectations(t)
		})
	}
}

func TestCoordinator_RegisterDataNode(t *testing.T) {
	cfg := config.DefaultCoordinatorConfig()
	logger := logging.NewTestLogger(slog.LevelError, true)
	testCases := []struct {
		name         string
		setupMocks   func(*serverMocks)
		req          *proto.RegisterDataNodeRequest
		expectedResp *proto.RegisterDataNodeResponse
		expectErr    bool
	}{
		{
			name: "success",
			setupMocks: func(mocks *serverMocks) {
				nodeInfo := &common.NodeInfo{ID: "node1", Host: "127.0.0.1", Port: 9001, Status: common.NodeHealthy}
				mocks.clusterStateHistoryManager.On("AddNode", mock.AnythingOfType("*common.NodeInfo")).Once()
				mocks.clusterStateHistoryManager.On("ListNodes", mock.Anything).Return([]*common.NodeInfo{nodeInfo}, int64(1)).Once()
			},
			req: &proto.RegisterDataNodeRequest{
				NodeInfo: &proto.NodeInfo{Id: "node1", IpAddress: "127.0.0.1", Port: 9001, Status: proto.NodeStatus_NODE_HEALTHY},
			},
			expectedResp: &proto.RegisterDataNodeResponse{
				Success: true,
				Message: "Node registered successfully",
				FullNodeList: []*proto.NodeInfo{
					{Id: "node1", IpAddress: "127.0.0.1", Port: 9001, Status: proto.NodeStatus_NODE_HEALTHY},
				},
				CurrentVersion: 1,
			},
			expectErr: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mocks := &serverMocks{
				clusterStateHistoryManager: new(state.MockClusterStateHistoryManager),
			}
			tc.setupMocks(mocks)
			container := NewContainer(mocks.metaStore, mocks.metadataManager, mocks.clusterStateHistoryManager, mocks.nodeSelector)
			coordinator := NewCoordinator(&cfg, container, logger)
			resp, err := coordinator.RegisterDataNode(context.Background(), tc.req)

			if tc.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expectedResp.FullNodeList[0].Id, resp.FullNodeList[0].Id)
				assert.Equal(t, tc.expectedResp.FullNodeList[0].IpAddress, resp.FullNodeList[0].IpAddress)
				assert.Equal(t, tc.expectedResp.FullNodeList[0].Port, resp.FullNodeList[0].Port)
				assert.Equal(t, tc.expectedResp.FullNodeList[0].Status, resp.FullNodeList[0].Status)
				assert.Equal(t, tc.expectedResp.CurrentVersion, resp.CurrentVersion)
				assert.Equal(t, tc.expectedResp.Message, resp.Message)
				assert.Equal(t, tc.expectedResp.Success, resp.Success)
			}
			mocks.clusterStateHistoryManager.AssertExpectations(t)
		})
	}
}

func TestCoordinator_DataNodeHeartbeat(t *testing.T) {
	cfg := config.DefaultCoordinatorConfig()
	logger := logging.NewTestLogger(slog.LevelError, true)
	testCases := []struct {
		name         string
		setupMocks   func(*serverMocks)
		req          *proto.HeartbeatRequest
		expectedResp *proto.HeartbeatResponse
		expectErr    bool
	}{
		{
			name: "success",
			setupMocks: func(mocks *serverMocks) {
				node := &common.NodeInfo{ID: "node1"}
				mocks.clusterStateHistoryManager.On("GetNode", "node1").Return(node, nil).Once()
				mocks.clusterStateHistoryManager.On("GetUpdatesSince", int64(1)).Return([]common.NodeUpdate{}, int64(1), nil).Once()
			},
			req: &proto.HeartbeatRequest{
				NodeId:          "node1",
				Status:          &proto.HealthStatus{Status: proto.NodeStatus_NODE_HEALTHY},
				LastSeenVersion: 1,
			},
			expectedResp: &proto.HeartbeatResponse{
				Success:     true,
				Message:     "ok",
				Updates:     []*proto.NodeUpdate{},
				ToVersion:   1,
				FromVersion: 1,
			},
			expectErr: false,
		},
		{
			name: "error: node not registered",
			setupMocks: func(mocks *serverMocks) {
				mocks.clusterStateHistoryManager.On("GetNode", "unknown-node").Return(nil, state.ErrNotFound).Once()
			},
			req: &proto.HeartbeatRequest{
				NodeId: "unknown-node",
				Status: &proto.HealthStatus{
					Status:   proto.NodeStatus_NODE_HEALTHY,
					LastSeen: timestamppb.New(time.Unix(0, 0)),
				},
				LastSeenVersion: 0,
			},
			expectedResp: &proto.HeartbeatResponse{
				Success: false,
				Message: "node is not registered",
				Updates: []*proto.NodeUpdate{},
			},
			expectErr: false,
		},
		{
			name: "error: version too old",
			setupMocks: func(mocks *serverMocks) {
				node := &common.NodeInfo{ID: "node1"}
				mocks.clusterStateHistoryManager.On("GetNode", "node1").Return(node, nil).Once()
				mocks.clusterStateHistoryManager.On("GetUpdatesSince", int64(0)).Return(nil, int64(5), assert.AnError).Once()
			},
			req: &proto.HeartbeatRequest{
				NodeId:          "node1",
				Status:          &proto.HealthStatus{Status: proto.NodeStatus_NODE_HEALTHY},
				LastSeenVersion: 0,
			},
			expectedResp: &proto.HeartbeatResponse{
				Success:            true,
				Message:            "version too old",
				FromVersion:        0,
				ToVersion:          5,
				RequiresFullResync: true,
				Updates:            []*proto.NodeUpdate{},
			},
			expectErr: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mocks := &serverMocks{
				clusterStateHistoryManager: new(state.MockClusterStateHistoryManager),
			}
			tc.setupMocks(mocks)
			container := NewContainer(mocks.metaStore, mocks.metadataManager, mocks.clusterStateHistoryManager, mocks.nodeSelector)
			coordinator := NewCoordinator(&cfg, container, logger)
			resp, err := coordinator.DataNodeHeartbeat(context.Background(), tc.req)

			if tc.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expectedResp.Success, resp.Success)
				assert.Equal(t, tc.expectedResp.Message, resp.Message)
				assert.Equal(t, tc.expectedResp.Updates, resp.Updates)
				assert.Equal(t, tc.expectedResp.FromVersion, resp.FromVersion)
				assert.Equal(t, tc.expectedResp.ToVersion, resp.ToVersion)
				assert.Equal(t, tc.expectedResp.RequiresFullResync, resp.RequiresFullResync)
			}
			mocks.clusterStateHistoryManager.AssertExpectations(t)
		})
	}
}

func TestCoordinator_ListNodes(t *testing.T) {
	cfg := config.DefaultCoordinatorConfig()
	logger := logging.NewTestLogger(slog.LevelError, true)
	testCases := []struct {
		name         string
		setupMocks   func(*serverMocks)
		req          *proto.ListNodesRequest
		expectedResp *proto.ListNodesResponse
		expectErr    bool
	}{
		{
			name: "success",
			setupMocks: func(mocks *serverMocks) {
				nodes := []*common.NodeInfo{
					{ID: "node1", Host: "localhost", Port: 9001, Status: common.NodeHealthy, LastSeen: time.Unix(0, 0)},
				}
				mocks.clusterStateHistoryManager.On("ListNodes", mock.Anything).Return(nodes, int64(1)).Once()
			},
			req: &proto.ListNodesRequest{},
			expectedResp: &proto.ListNodesResponse{
				Nodes: []*proto.NodeInfo{
					{Id: "node1", IpAddress: "localhost", Port: 9001, Status: proto.NodeStatus_NODE_HEALTHY, LastSeen: timestamppb.New(time.Unix(0, 0))},
				},
				CurrentVersion: 1,
			},
			expectErr: false,
		},
		{
			name: "success: no nodes",
			setupMocks: func(mocks *serverMocks) {
				mocks.clusterStateHistoryManager.On("ListNodes", mock.Anything).Return([]*common.NodeInfo{}, int64(0)).Once()
			},
			req: &proto.ListNodesRequest{},
			expectedResp: &proto.ListNodesResponse{
				Nodes:          []*proto.NodeInfo{},
				CurrentVersion: 0,
			},
			expectErr: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mocks := &serverMocks{
				clusterStateHistoryManager: new(state.MockClusterStateHistoryManager),
			}
			tc.setupMocks(mocks)
			container := NewContainer(mocks.metaStore, mocks.metadataManager, mocks.clusterStateHistoryManager, mocks.nodeSelector)
			coordinator := NewCoordinator(&cfg, container, logger)
			resp, err := coordinator.ListNodes(context.Background(), tc.req)

			if tc.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expectedResp, resp)
			}
			mocks.clusterStateHistoryManager.AssertExpectations(t)
		})
	}
}
