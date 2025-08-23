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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type serviceMocks struct {
	clusterStateHistoryManager *state.MockClusterStateHistoryManager
	nodeSelector               *state.MockNodeSelector
	metaStore                  *metadata.MockMetadataStore
	metadataManager            *MockMetadataSessionManager
}

func (m *serviceMocks) assertExpectations(t *testing.T) {
	m.clusterStateHistoryManager.AssertExpectations(t)
	m.nodeSelector.AssertExpectations(t)
	m.metaStore.AssertExpectations(t)
	m.metadataManager.AssertExpectations(t)
}

func TestService_UploadFile(t *testing.T) {
	cfg := config.CoordinatorConfig{
		ChunkSize: 1 * 1024 * 1024, // 1MB
		Metadata: config.MetadataConfig{
			CommitTimeout: 5 * time.Second,
		},
	}
	logger := logging.NewTestLogger(slog.LevelError, true)

	testCases := []struct {
		name         string
		setupMocks   func(*serviceMocks)
		req          common.UploadRequest
		expectedResp func(sessionID common.MetadataSessionID) common.UploadResponse
		expectErr    bool
	}{
		{
			name: "success: 1 chunk",
			setupMocks: func(mocks *serviceMocks) {
				mocks.nodeSelector.On("SelectBestNodes", 1, &common.NodeInfo{ID: "coordinator"}).Return([]*common.NodeInfo{
					{ID: "node1", Status: common.NodeHealthy, Host: "127.0.0.1", Port: 8081},
				}, nil)
				mocks.metadataManager.On("trackUpload", mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
			},
			req: common.UploadRequest{
				Path:      "test.txt",
				ChunkSize: 1 * 1024 * 1024,
				Size:      (1 * 1024 * 1024) - 1, // 1MB - 1B
				Checksum:  "test-checksum",
			},
			expectedResp: func(sessionID common.MetadataSessionID) common.UploadResponse {
				return common.UploadResponse{
					SessionID: sessionID,
					ChunkIDs:  []string{chunk.FormatChunkID("test.txt", 0)},
					Nodes:     []*common.NodeInfo{{ID: "node1", Status: common.NodeHealthy, Host: "127.0.0.1", Port: 8081}},
				}
			},
			expectErr: false,
		},
		{
			name: "success: 2 chunks",
			setupMocks: func(mocks *serviceMocks) {
				mocks.nodeSelector.On("SelectBestNodes", 2, &common.NodeInfo{ID: "coordinator"}).Return([]*common.NodeInfo{
					{ID: "node1", Status: common.NodeHealthy, Host: "127.0.0.1", Port: 8081},
				}, nil)
				mocks.metadataManager.On("trackUpload", mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
			},
			req: common.UploadRequest{
				Path:      "test.txt",
				ChunkSize: 1 * 1024 * 1024,
				Size:      (2 * 1024 * 1024) - 1, // 2MB - 1B
				Checksum:  "test-checksum",
			},
			expectedResp: func(sessionID common.MetadataSessionID) common.UploadResponse {
				return common.UploadResponse{
					SessionID: sessionID,
					ChunkIDs:  []string{chunk.FormatChunkID("test.txt", 0), chunk.FormatChunkID("test.txt", 1)},
					Nodes:     []*common.NodeInfo{{ID: "node1", Status: common.NodeHealthy, Host: "127.0.0.1", Port: 8081}},
				}
			},
			expectErr: false,
		},
		{
			name: "error: node selector not ok",
			setupMocks: func(mocks *serviceMocks) {
				mocks.nodeSelector.On("SelectBestNodes", 1, &common.NodeInfo{ID: "coordinator"}).Return(nil, state.ErrNoAvailableNodes)
			},
			req: common.UploadRequest{
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
			mocks := &serviceMocks{
				nodeSelector:               new(state.MockNodeSelector),
				metaStore:                  new(metadata.MockMetadataStore),
				clusterStateHistoryManager: new(state.MockClusterStateHistoryManager),
				metadataManager:            new(MockMetadataSessionManager),
			}
			tc.setupMocks(mocks)
			container := NewContainer(mocks.metaStore, mocks.metadataManager, mocks.clusterStateHistoryManager, mocks.nodeSelector)
			service := NewService(&cfg, container)

			ctx := logging.WithLogger(context.Background(), logger)

			resp, err := service.uploadFile(ctx, tc.req)
			time.Sleep(1 * time.Second) // wait for metadataManager goroutine to start and finish, this is a hack to avoid race conditions

			if tc.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				expected := tc.expectedResp(resp.SessionID)
				assert.Equal(t, expected.ChunkIDs, resp.ChunkIDs)
				assert.Equal(t, expected.Nodes, resp.Nodes)
				assert.NotEmpty(t, resp.SessionID)
			}

			mocks.assertExpectations(t)
		})
	}
}

func TestService_DownloadFile(t *testing.T) {
	cfg := config.DefaultCoordinatorConfig()
	logger := logging.NewTestLogger(slog.LevelError, true)

	testCases := []struct {
		name         string
		setupMocks   func(*serviceMocks)
		req          common.DownloadRequest
		expectedResp common.DownloadResponse
		expectErr    bool
	}{
		{
			name: "success",
			setupMocks: func(mocks *serviceMocks) {
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
			req: common.DownloadRequest{Path: "test.txt"},
			expectedResp: common.DownloadResponse{
				FileInfo: common.FileInfo{
					Path:       "test.txt",
					Size:       1 * 1024 * 1024,
					ChunkCount: 1,
					Chunks:     []common.ChunkInfo{{Header: common.ChunkHeader{ID: "chunk1"}, Replicas: []*common.NodeInfo{{ID: "node1"}}}},
					CreatedAt:  time.Unix(0, 0),
					Checksum:   "test-checksum",
				},
				ChunkLocations: []common.ChunkLocation{
					{ChunkID: "chunk1", Nodes: []*common.NodeInfo{{ID: "node1", Host: "localhost", Port: 9001, Status: common.NodeHealthy}}},
				},
			},
			expectErr: false,
		},
		{
			name: "success: 2 chunks",
			setupMocks: func(mocks *serviceMocks) {
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
			req: common.DownloadRequest{Path: "test.txt"},
			expectedResp: common.DownloadResponse{
				FileInfo: common.FileInfo{
					Path:       "test.txt",
					Size:       2 * 1024 * 1024,
					ChunkCount: 2,
					Chunks: []common.ChunkInfo{
						{Header: common.ChunkHeader{ID: "chunk1"}, Replicas: []*common.NodeInfo{{ID: "node1"}}},
						{Header: common.ChunkHeader{ID: "chunk2"}, Replicas: []*common.NodeInfo{{ID: "node1"}}},
					},
					CreatedAt: time.Unix(0, 0),
				},
				ChunkLocations: []common.ChunkLocation{
					{ChunkID: "chunk1", Nodes: []*common.NodeInfo{{ID: "node1", Host: "localhost", Port: 9001, Status: common.NodeHealthy}}},
					{ChunkID: "chunk2", Nodes: []*common.NodeInfo{{ID: "node1", Host: "localhost", Port: 9001, Status: common.NodeHealthy}}},
				},
			},
			expectErr: false,
		},
		{
			name: "error: file not found",
			setupMocks: func(mocks *serviceMocks) {
				mocks.metaStore.On("GetFile", mock.Anything, "test.txt").Return(nil, errors.New("file not found")).Once()
			},
			req:          common.DownloadRequest{Path: "test.txt"},
			expectedResp: common.DownloadResponse{},
			expectErr:    true,
		},
		{
			name: "error: no available nodes",
			setupMocks: func(mocks *serviceMocks) {
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
			req:          common.DownloadRequest{Path: "test.txt"},
			expectedResp: common.DownloadResponse{},
			expectErr:    true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mocks := &serviceMocks{
				clusterStateHistoryManager: new(state.MockClusterStateHistoryManager),
				metaStore:                  new(metadata.MockMetadataStore),
				metadataManager:            new(MockMetadataSessionManager),
				nodeSelector:               new(state.MockNodeSelector),
			}
			tc.setupMocks(mocks)
			container := NewContainer(mocks.metaStore, mocks.metadataManager, mocks.clusterStateHistoryManager, mocks.nodeSelector)
			service := NewService(&cfg, container)

			ctx := logging.WithLogger(context.Background(), logger)
			resp, err := service.downloadFile(ctx, tc.req)

			if tc.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expectedResp.FileInfo, resp.FileInfo)
				for i, expected := range tc.expectedResp.ChunkLocations {
					assert.Equal(t, expected.ChunkID, resp.ChunkLocations[i].ChunkID)
					for j, node := range expected.Nodes {
						assert.Equal(t, node.ID, resp.ChunkLocations[i].Nodes[j].ID)
						assert.Equal(t, node.Host, resp.ChunkLocations[i].Nodes[j].Host)
						assert.Equal(t, node.Port, resp.ChunkLocations[i].Nodes[j].Port)
					}
				}
				assert.NotEmpty(t, resp.SessionID)
			}

			mocks.metaStore.AssertExpectations(t)
			mocks.clusterStateHistoryManager.AssertExpectations(t)
		})
	}
}

func TestService_DeleteFile(t *testing.T) {
	cfg := config.DefaultCoordinatorConfig()
	logger := logging.NewTestLogger(slog.LevelError, true)

	testCases := []struct {
		name         string
		setupMocks   func(*serviceMocks)
		req          common.DeleteRequest
		expectedResp common.DeleteResponse
		expectErr    bool
	}{
		{
			name: "success",
			setupMocks: func(mocks *serviceMocks) {
				mocks.metaStore.On("DeleteFile", mock.Anything, "test.txt").Return(nil).Once()
			},
			req:          common.DeleteRequest{Path: "test.txt"},
			expectedResp: common.DeleteResponse{Success: true},
			expectErr:    false,
		},
		{
			name: "error: delete fails",
			setupMocks: func(mocks *serviceMocks) {
				mocks.metaStore.On("DeleteFile", mock.Anything, "test.txt").Return(assert.AnError).Once()
			},
			req:          common.DeleteRequest{Path: "test.txt"},
			expectedResp: common.DeleteResponse{},
			expectErr:    true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mocks := &serviceMocks{
				metaStore: new(metadata.MockMetadataStore),
			}
			tc.setupMocks(mocks)
			container := NewContainer(mocks.metaStore, mocks.metadataManager, mocks.clusterStateHistoryManager, mocks.nodeSelector)
			service := NewService(&cfg, container)

			ctx := logging.WithLogger(context.Background(), logger)
			resp, err := service.deleteFile(ctx, tc.req)

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

func TestService_ConfirmUpload(t *testing.T) {
	cfg := config.DefaultCoordinatorConfig()
	logger := logging.NewTestLogger(slog.LevelError, true)

	testCases := []struct {
		name         string
		setupMocks   func(*serviceMocks)
		req          common.ConfirmUploadRequest
		expectedResp common.ConfirmUploadResponse
		expectErr    bool
		expectedErr  error
	}{
		{
			name: "success",
			setupMocks: func(mocks *serviceMocks) {
				mocks.metadataManager.On("commit", mock.Anything, common.MetadataSessionID("test-session"), mock.Anything, mocks.metaStore).Return(nil).Once()
			},
			req: common.ConfirmUploadRequest{
				SessionID: common.MetadataSessionID("test-session"),
			},
			expectedResp: common.ConfirmUploadResponse{Success: true},
			expectErr:    false,
			expectedErr:  nil,
		},
		{
			name: "error: commit fails",
			setupMocks: func(mocks *serviceMocks) {
				mocks.metadataManager.On("commit", mock.Anything, common.MetadataSessionID("test-session-fail"), mock.Anything, mocks.metaStore).Return(assert.AnError).Once()
			},
			req: common.ConfirmUploadRequest{
				SessionID: common.MetadataSessionID("test-session-fail"),
			},
			expectedResp: common.ConfirmUploadResponse{},
			expectErr:    true,
			expectedErr:  assert.AnError,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mocks := &serviceMocks{
				metaStore:       new(metadata.MockMetadataStore),
				metadataManager: new(MockMetadataSessionManager),
			}
			tc.setupMocks(mocks)
			container := NewContainer(mocks.metaStore, mocks.metadataManager, mocks.clusterStateHistoryManager, mocks.nodeSelector)
			service := NewService(&cfg, container)
			ctx := logging.WithLogger(context.Background(), logger)
			resp, err := service.confirmUpload(ctx, tc.req)

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

func TestService_RegisterDataNode(t *testing.T) {
	cfg := config.DefaultCoordinatorConfig()
	logger := logging.NewTestLogger(slog.LevelError, true)
	testCases := []struct {
		name         string
		setupMocks   func(*serviceMocks)
		req          common.RegisterDataNodeRequest
		expectedResp common.RegisterDataNodeResponse
		expectErr    bool
	}{
		{
			name: "success",
			setupMocks: func(mocks *serviceMocks) {
				nodeInfo := &common.NodeInfo{ID: "node1", Host: "127.0.0.1", Port: 9001, Status: common.NodeHealthy}
				mocks.clusterStateHistoryManager.On("AddNode", mock.AnythingOfType("*common.NodeInfo")).Once()
				mocks.clusterStateHistoryManager.On("ListNodes", mock.Anything).Return([]*common.NodeInfo{nodeInfo}, int64(1)).Once()
			},
			req: common.RegisterDataNodeRequest{
				NodeInfo: &common.NodeInfo{ID: "node1", Host: "127.0.0.1", Port: 9001, Status: common.NodeHealthy},
			},
			expectedResp: common.RegisterDataNodeResponse{
				Success: true,
				Message: "Node registered successfully",
				FullNodeList: []*common.NodeInfo{
					{ID: "node1", Host: "127.0.0.1", Port: 9001, Status: common.NodeHealthy},
				},
				CurrentVersion: 1,
			},
			expectErr: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mocks := &serviceMocks{
				clusterStateHistoryManager: new(state.MockClusterStateHistoryManager),
			}
			tc.setupMocks(mocks)
			container := NewContainer(mocks.metaStore, mocks.metadataManager, mocks.clusterStateHistoryManager, mocks.nodeSelector)
			service := NewService(&cfg, container)
			ctx := logging.WithLogger(context.Background(), logger)
			resp, err := service.registerDataNode(ctx, tc.req)

			if tc.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expectedResp.FullNodeList[0].ID, resp.FullNodeList[0].ID)
				assert.Equal(t, tc.expectedResp.FullNodeList[0].Host, resp.FullNodeList[0].Host)
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

func TestService_DataNodeHeartbeat(t *testing.T) {
	cfg := config.DefaultCoordinatorConfig()
	logger := logging.NewTestLogger(slog.LevelError, true)
	testCases := []struct {
		name         string
		setupMocks   func(*serviceMocks)
		req          common.HeartbeatRequest
		expectedResp common.HeartbeatResponse
		expectErr    bool
		expectedErr  error
	}{
		{
			name: "success",
			setupMocks: func(mocks *serviceMocks) {
				node := &common.NodeInfo{ID: "node1"}
				mocks.clusterStateHistoryManager.On("GetNode", "node1").Return(node, nil).Once()
				mocks.clusterStateHistoryManager.On("GetUpdatesSince", int64(1)).Return([]common.NodeUpdate{}, int64(1), nil).Once()
			},
			req: common.HeartbeatRequest{
				NodeID:          "node1",
				Status:          common.HealthStatus{Status: common.NodeHealthy},
				LastSeenVersion: 1,
			},
			expectedResp: common.HeartbeatResponse{
				Success:     true,
				Message:     "ok",
				Updates:     []common.NodeUpdate{},
				ToVersion:   1,
				FromVersion: 1,
			},
			expectErr:   false,
			expectedErr: nil,
		},
		{
			name: "error: node not registered",
			setupMocks: func(mocks *serviceMocks) {
				mocks.clusterStateHistoryManager.On("GetNode", "unknown-node").Return(nil, state.ErrNotFound).Once()
			},
			req: common.HeartbeatRequest{
				NodeID: "unknown-node",
				Status: common.HealthStatus{
					Status:   common.NodeHealthy,
					LastSeen: time.Unix(0, 0),
				},
				LastSeenVersion: 0,
			},
			expectedResp: common.HeartbeatResponse{
				Success: false,
				Message: "node is not registered",
			},
			expectErr:   false,
			expectedErr: nil,
		},
		{
			name: "error: version too old",
			setupMocks: func(mocks *serviceMocks) {
				node := &common.NodeInfo{ID: "node1"}
				mocks.clusterStateHistoryManager.On("GetNode", "node1").Return(node, nil).Once()
				mocks.clusterStateHistoryManager.On("GetUpdatesSince", int64(0)).Return(nil, int64(5), assert.AnError).Once()
			},
			req: common.HeartbeatRequest{
				NodeID:          "node1",
				Status:          common.HealthStatus{Status: common.NodeHealthy},
				LastSeenVersion: 0,
			},
			expectedResp: common.HeartbeatResponse{
				Success:            true,
				Message:            "version too old",
				FromVersion:        0,
				ToVersion:          5,
				RequiresFullResync: true,
			},
			expectErr:   false,
			expectedErr: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mocks := &serviceMocks{
				clusterStateHistoryManager: new(state.MockClusterStateHistoryManager),
			}
			tc.setupMocks(mocks)
			container := NewContainer(mocks.metaStore, mocks.metadataManager, mocks.clusterStateHistoryManager, mocks.nodeSelector)
			service := NewService(&cfg, container)
			ctx := logging.WithLogger(context.Background(), logger)
			resp, err := service.heartbeat(ctx, tc.req)

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

func TestService_ListNodes(t *testing.T) {
	cfg := config.DefaultCoordinatorConfig()
	logger := logging.NewTestLogger(slog.LevelError, true)
	testCases := []struct {
		name         string
		setupMocks   func(*serviceMocks)
		req          common.ListNodesRequest
		expectedResp common.ListNodesResponse
		expectErr    bool
	}{
		{
			name: "success",
			setupMocks: func(mocks *serviceMocks) {
				nodes := []*common.NodeInfo{
					{ID: "node1", Host: "localhost", Port: 9001, Status: common.NodeHealthy, LastSeen: time.Unix(0, 0)},
				}
				mocks.clusterStateHistoryManager.On("ListNodes", mock.Anything).Return(nodes, int64(1)).Once()
			},
			req: common.ListNodesRequest{},
			expectedResp: common.ListNodesResponse{
				Nodes: []*common.NodeInfo{
					{ID: "node1", Host: "localhost", Port: 9001, Status: common.NodeHealthy, LastSeen: time.Unix(0, 0)},
				},
				CurrentVersion: 1,
			},
			expectErr: false,
		},
		{
			name: "success: no nodes",
			setupMocks: func(mocks *serviceMocks) {
				mocks.clusterStateHistoryManager.On("ListNodes", mock.Anything).Return([]*common.NodeInfo{}, int64(0)).Once()
			},
			req: common.ListNodesRequest{},
			expectedResp: common.ListNodesResponse{
				Nodes:          []*common.NodeInfo{},
				CurrentVersion: 0,
			},
			expectErr: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mocks := &serviceMocks{
				clusterStateHistoryManager: new(state.MockClusterStateHistoryManager),
			}
			tc.setupMocks(mocks)
			container := NewContainer(mocks.metaStore, mocks.metadataManager, mocks.clusterStateHistoryManager, mocks.nodeSelector)
			service := NewService(&cfg, container)
			ctx := logging.WithLogger(context.Background(), logger)
			resp, err := service.listNodes(ctx, tc.req)

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
