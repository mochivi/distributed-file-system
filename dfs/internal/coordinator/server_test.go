package coordinator

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mochivi/distributed-file-system/internal/cluster/state"
	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/internal/storage/chunk"
	"github.com/mochivi/distributed-file-system/internal/storage/metadata"
	"github.com/mochivi/distributed-file-system/pkg/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type serverMocks struct {
	service *MockService
}

func (m *serverMocks) assertExpectations(t *testing.T) {
	m.service.AssertExpectations(t)
}

func TestCoordinator_UploadFile(t *testing.T) {
	defaultTestReq := &proto.UploadRequest{
		Path:      "test.txt",
		ChunkSize: 1 * 1024 * 1024,       // 1MB chunksize
		Size:      (1 * 1024 * 1024) - 1, // 1MB - 1B (only one chunk required)
		Checksum:  "test-checksum",
	}

	testCases := []struct {
		name         string
		setupMock    func(*serverMocks)
		req          *proto.UploadRequest
		expectedResp *proto.UploadResponse
		expectedErr  error
		expectErr    bool
	}{
		{
			name: "success",
			setupMock: func(mocks *serverMocks) {
				mocks.service.On("uploadFile", mock.Anything, mock.Anything).
					Return(common.UploadResponse{
						SessionID: "test-session",
						ChunkIDs:  []string{chunk.FormatChunkID("test.txt", 0)},
						Nodes:     []*common.NodeInfo{{ID: "node1", Status: common.NodeHealthy, Host: "127.0.0.1", Port: 8081}},
					}, nil).Once()
			},
			req: defaultTestReq,
			expectedResp: &proto.UploadResponse{
				SessionId: "test-session",
				ChunkIds:  []string{chunk.FormatChunkID("test.txt", 0)},
				Nodes:     []*proto.NodeInfo{{Id: "node1", Status: proto.NodeStatus_NODE_HEALTHY, IpAddress: "127.0.0.1", Port: 8081}},
			},
			expectedErr: nil,
			expectErr:   false,
		},
		{
			name:      "error: validation: chunksize too small",
			setupMock: func(mocks *serverMocks) {},
			req: &proto.UploadRequest{
				Path:      "test.txt",
				ChunkSize: 0.5 * 1024 * 1024, // 0.5MB chunksize
				Size:      (1 * 1024 * 1024) - 1,
				Checksum:  "test-checksum",
			},
			expectedResp: nil,
			expectedErr:  common.ErrValidation,
			expectErr:    true,
		},
		{
			name:      "error: validation: chunksize too large",
			setupMock: func(mocks *serverMocks) {},
			req: &proto.UploadRequest{
				Path:      "test.txt",
				ChunkSize: 129 * 1024 * 1024, // 129MB chunksize
				Size:      (1 * 1024 * 1024) - 1,
				Checksum:  "test-checksum",
			},
			expectedResp: nil,
			expectedErr:  common.ErrValidation,
			expectErr:    true,
		},
		{
			name: "error: no available nodes",
			setupMock: func(mocks *serverMocks) {
				mocks.service.On("uploadFile", mock.Anything, mock.Anything).
					Return(common.UploadResponse{}, state.ErrNoAvailableNodes).Once()
			},
			req:          defaultTestReq,
			expectedResp: nil,
			expectedErr:  state.ErrNoAvailableNodes,
			expectErr:    true,
		},
		{
			name: "error: unexpected error",
			setupMock: func(mocks *serverMocks) {
				mocks.service.On("uploadFile", mock.Anything, mock.Anything).
					Return(common.UploadResponse{}, assert.AnError).Once()
			},
			req:          defaultTestReq,
			expectedResp: nil,
			expectedErr:  assert.AnError,
			expectErr:    true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mocks := &serverMocks{service: &MockService{}}
			tc.setupMock(mocks)

			coordinator := NewCoordinator(mocks.service)
			resp, err := coordinator.UploadFile(context.Background(), tc.req)

			if tc.expectErr {
				assert.Error(t, err)
				assert.ErrorIs(t, err, tc.expectedErr)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expectedResp.SessionId, resp.SessionId)
				assert.ElementsMatch(t, tc.expectedResp.ChunkIds, resp.ChunkIds)
				assert.ElementsMatch(t, tc.expectedResp.Nodes, resp.Nodes)
			}

			mocks.assertExpectations(t)
		})
	}
}

func TestCoordinator_DownloadFile(t *testing.T) {
	req := &proto.DownloadRequest{Path: "test.txt"}
	testCases := []struct {
		name         string
		setupMocks   func(*serverMocks)
		expectedResp *proto.DownloadResponse
		expectErr    bool
		expectedErr  error
	}{
		{
			name: "success",
			setupMocks: func(mocks *serverMocks) {
				resp := common.DownloadResponse{
					FileInfo: common.FileInfo{
						Path:       "test.txt",
						Size:       1 * 1024 * 1024,
						ChunkCount: 1,
						Chunks: []common.ChunkInfo{
							{Header: common.ChunkHeader{ID: "chunk1"}, Replicas: []*common.NodeInfo{{ID: "node1"}}},
						},
						CreatedAt: time.Unix(0, 0),
						Checksum:  "test-checksum",
					},
					ChunkLocations: []common.ChunkLocation{
						{ChunkID: "chunk1", Nodes: []*common.NodeInfo{
							{ID: "node1", Status: common.NodeHealthy, Host: "localhost", Port: 9001},
						}},
					},
					SessionID: "test-session",
				}
				mocks.service.On("downloadFile", mock.Anything, mock.Anything).Return(resp, nil).Once()
			},
			expectedResp: &proto.DownloadResponse{
				FileInfo: (&common.FileInfo{
					Path:       "test.txt",
					Size:       1 * 1024 * 1024,
					ChunkCount: 1,
					Chunks: []common.ChunkInfo{
						{Header: common.ChunkHeader{ID: "chunk1"}, Replicas: []*common.NodeInfo{{ID: "node1"}}},
					},
					CreatedAt: time.Unix(0, 0),
					Checksum:  "test-checksum",
				}).ToProto(),
				ChunkLocations: []*proto.ChunkLocation{
					{ChunkId: "chunk1", Nodes: []*proto.NodeInfo{
						{Id: "node1", IpAddress: "localhost", Port: 9001, Status: proto.NodeStatus_NODE_HEALTHY},
					}},
				},
				SessionId: "test-session",
			},
			expectErr:   false,
			expectedErr: nil,
		},
		{
			name: "error: file not in metadata store",
			setupMocks: func(mocks *serverMocks) {
				mocks.service.On("downloadFile", mock.Anything, mock.Anything).
					Return(common.DownloadResponse{}, errors.New("file not found")).Once()
			},
			expectedResp: nil,
			expectedErr:  metadata.ErrNotFound,
			expectErr:    true,
		},
		{
			name: "error: no available nodes",
			setupMocks: func(mocks *serverMocks) {
				mocks.service.On("downloadFile", mock.Anything, mock.Anything).
					Return(common.DownloadResponse{}, state.ErrNoAvailableNodes).Once()
			},
			expectedResp: nil,
			expectErr:    true,
		},
		{
			name: "error: unexpected error",
			setupMocks: func(mocks *serverMocks) {
				mocks.service.On("downloadFile", mock.Anything, mock.Anything).
					Return(common.DownloadResponse{}, assert.AnError).Once()
			},
			expectedResp: nil,
			expectErr:    true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mocks := &serverMocks{service: &MockService{}}
			tc.setupMocks(mocks)

			coordinator := NewCoordinator(mocks.service)
			resp, err := coordinator.DownloadFile(context.Background(), req)

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

			mocks.assertExpectations(t)
		})
	}
}

func TestCoordinator_ListFiles(t *testing.T) {
	req := &proto.ListRequest{Directory: "test"}

	testCases := []struct {
		name        string
		setupMocks  func(*serverMocks)
		req         *proto.ListRequest
		expectErr   bool
		expectedErr error
	}{
		{
			name: "success",
			setupMocks: func(mocks *serverMocks) {
				resp := common.ListResponse{
					Files: []*common.FileInfo{
						{Path: "test.txt", Size: 1 * 1024 * 1024, ChunkCount: 1, CreatedAt: time.Unix(0, 0), Checksum: "test-checksum",
							Chunks: []common.ChunkInfo{
								{Header: common.ChunkHeader{ID: "chunk1"}, Replicas: []*common.NodeInfo{{ID: "node1"}}},
							},
						},
					},
				}
				mocks.service.On("listFiles", mock.Anything, mock.Anything).Return(resp, nil).Once()
			},
			expectErr:   false,
			expectedErr: nil,
		},
		{
			name: "error: unexpected error",
			setupMocks: func(mocks *serverMocks) {
				mocks.service.On("listFiles", mock.Anything, mock.Anything).Return(common.ListResponse{}, assert.AnError).Once()
			},
			expectErr:   true,
			expectedErr: assert.AnError,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mocks := &serverMocks{service: &MockService{}}
			tc.setupMocks(mocks)

			coordinator := NewCoordinator(mocks.service)
			_, err := coordinator.ListFiles(context.Background(), req)

			if tc.expectErr {
				assert.Error(t, err)
				assert.ErrorIs(t, err, tc.expectedErr)
			} else {
				assert.NoError(t, err)
			}

			mocks.assertExpectations(t)
		})
	}
}

func TestCoordinator_DeleteFile(t *testing.T) {
	req := &proto.DeleteRequest{Path: "test.txt"}
	testCases := []struct {
		name         string
		setupMocks   func(*serverMocks)
		expectedResp *proto.DeleteResponse
		expectErr    bool
		expectedErr  error
	}{
		{
			name: "success",
			setupMocks: func(mocks *serverMocks) {
				protoResp := &proto.DeleteResponse{Success: true}
				mocks.service.On("deleteFile", mock.Anything, mock.Anything).
					Return(common.DeleteResponseFromProto(protoResp), nil).Once()
			},
			expectedResp: &proto.DeleteResponse{Success: true},
			expectErr:    false,
			expectedErr:  nil,
		},
		{
			name: "error: file not found",
			setupMocks: func(mocks *serverMocks) {
				mocks.service.On("deleteFile", mock.Anything, mock.Anything).
					Return(common.DeleteResponse{}, metadata.ErrNotFound).Once()
			},
			expectedResp: nil,
			expectErr:    true,
			expectedErr:  metadata.ErrNotFound,
		},
		{
			name: "error: unexpected error",
			setupMocks: func(mocks *serverMocks) {
				mocks.service.On("deleteFile", mock.Anything, mock.Anything).
					Return(common.DeleteResponse{}, assert.AnError).Once()
			},
			expectedResp: nil,
			expectErr:    true,
			expectedErr:  assert.AnError,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mocks := &serverMocks{service: &MockService{}}
			tc.setupMocks(mocks)

			coordinator := NewCoordinator(mocks.service)
			resp, err := coordinator.DeleteFile(context.Background(), req)

			if tc.expectErr {
				assert.Error(t, err)
				assert.ErrorIs(t, err, tc.expectedErr)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expectedResp, resp)
			}

			mocks.assertExpectations(t)
		})
	}
}

func TestCoordinator_ConfirmUpload(t *testing.T) {
	req := &proto.ConfirmUploadRequest{SessionId: "test-session"}
	testCases := []struct {
		name         string
		setupMocks   func(*serverMocks)
		expectedResp *proto.ConfirmUploadResponse
		expectErr    bool
		expectedErr  error
	}{
		{
			name: "success",
			setupMocks: func(mocks *serverMocks) {
				protoResp := &proto.ConfirmUploadResponse{Success: true}
				mocks.service.On("confirmUpload", mock.Anything, mock.Anything).
					Return(common.ConfirmUploadResponseFromProto(protoResp), nil).Once()
			},
			expectedResp: &proto.ConfirmUploadResponse{Success: true},
			expectErr:    false,
			expectedErr:  nil,
		},
		{
			name: "error: unexpected error",
			setupMocks: func(mocks *serverMocks) {
				protoResp := &proto.ConfirmUploadResponse{Success: false, Message: "Failed to commit metadata"}
				mocks.service.On("confirmUpload", mock.Anything, mock.Anything).
					Return(common.ConfirmUploadResponseFromProto(protoResp), nil).Once()
			},
			expectedResp: &proto.ConfirmUploadResponse{Success: false, Message: "Failed to commit metadata"},
			expectErr:    false,
			expectedErr:  assert.AnError,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mocks := &serverMocks{service: &MockService{}}
			tc.setupMocks(mocks)

			coordinator := NewCoordinator(mocks.service)
			resp, err := coordinator.ConfirmUpload(context.Background(), req)

			if tc.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expectedResp, resp)
			}

			mocks.assertExpectations(t)
		})
	}
}

func TestCoordinator_RegisterDataNode(t *testing.T) {
	req := &proto.RegisterDataNodeRequest{
		NodeInfo: &proto.NodeInfo{Id: "node1", IpAddress: "127.0.0.1", Port: 9001, Status: proto.NodeStatus_NODE_HEALTHY},
	}
	testCases := []struct {
		name         string
		setupMocks   func(*serverMocks)
		req          *proto.RegisterDataNodeRequest
		expectedResp *proto.RegisterDataNodeResponse
		expectErr    bool
		expectedErr  error
	}{
		{
			name: "success",
			setupMocks: func(mocks *serverMocks) {
				protoResp := &proto.RegisterDataNodeResponse{
					Success:        true,
					Message:        "Node registered successfully",
					FullNodeList:   []*proto.NodeInfo{{Id: "node1", IpAddress: "127.0.0.1", Port: 9001, Status: proto.NodeStatus_NODE_HEALTHY}},
					CurrentVersion: 1,
				}
				mocks.service.On("registerDataNode", mock.Anything, mock.Anything).
					Return(common.RegisterDataNodeResponseFromProto(protoResp), nil).Once()
			},
			req: &proto.RegisterDataNodeRequest{NodeInfo: &proto.NodeInfo{Id: "node1", IpAddress: "127.0.0.1", Port: 9001, Status: proto.NodeStatus_NODE_HEALTHY}},
			expectedResp: &proto.RegisterDataNodeResponse{
				Success:        true,
				Message:        "Node registered successfully",
				FullNodeList:   []*proto.NodeInfo{{Id: "node1", IpAddress: "127.0.0.1", Port: 9001, Status: proto.NodeStatus_NODE_HEALTHY}},
				CurrentVersion: 1,
			},
			expectErr: false,
		},
		{
			name: "error: unexpected error",
			setupMocks: func(mocks *serverMocks) {
				mocks.service.On("registerDataNode", mock.Anything, mock.Anything).
					Return(common.RegisterDataNodeResponse{}, assert.AnError).Once()
			},
			expectErr:   true,
			expectedErr: assert.AnError,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mocks := &serverMocks{service: &MockService{}}
			tc.setupMocks(mocks)

			coordinator := NewCoordinator(mocks.service)
			resp, err := coordinator.RegisterDataNode(context.Background(), req)

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

			mocks.assertExpectations(t)
		})
	}
}

func TestCoordinator_DataNodeHeartbeat(t *testing.T) {
	testCases := []struct {
		name         string
		setupMocks   func(*serverMocks)
		req          *proto.HeartbeatRequest
		expectedResp *proto.HeartbeatResponse
		expectErr    bool
		expectedErr  error
	}{
		{
			name: "success",
			setupMocks: func(mocks *serverMocks) {
				protoResp := &proto.HeartbeatResponse{
					Success:     true,
					Message:     "ok",
					Updates:     []*proto.NodeUpdate{},
					FromVersion: 1,
					ToVersion:   1,
				}
				mocks.service.On("heartbeat", mock.Anything, mock.Anything).
					Return(common.HeartbeatResponseFromProto(protoResp), nil).Once()
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
				FromVersion: 1,
				ToVersion:   1,
			},
			expectErr:   false,
			expectedErr: nil,
		},
		{
			name: "error: node not found",
			setupMocks: func(mocks *serverMocks) {
				mocks.service.On("heartbeat", mock.Anything, mock.Anything).
					Return(common.HeartbeatResponse{}, state.ErrNotFound).Once()
			},
			req: &proto.HeartbeatRequest{
				NodeId:          "unknown-node",
				Status:          &proto.HealthStatus{Status: proto.NodeStatus_NODE_HEALTHY, LastSeen: timestamppb.New(time.Unix(0, 0))},
				LastSeenVersion: 0,
			},
			expectedResp: &proto.HeartbeatResponse{
				Success: false,
				Message: "node is not registered",
				Updates: []*proto.NodeUpdate{},
			},
			expectErr:   false,
			expectedErr: nil,
		},
		{
			name: "error: version too old",
			setupMocks: func(mocks *serverMocks) {
				protoResp := &proto.HeartbeatResponse{
					Success:            true,
					Message:            "version too old",
					FromVersion:        0,
					ToVersion:          5,
					RequiresFullResync: true,
					Updates:            []*proto.NodeUpdate{},
				}
				mocks.service.On("heartbeat", mock.Anything, mock.Anything).
					Return(common.HeartbeatResponseFromProto(protoResp), nil).Once()
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
			expectErr:   false,
			expectedErr: nil,
		},
		{
			name: "error: unexpected error",
			setupMocks: func(mocks *serverMocks) {
				mocks.service.On("heartbeat", mock.Anything, mock.Anything).
					Return(common.HeartbeatResponse{}, assert.AnError).Once()
			},
			req: &proto.HeartbeatRequest{
				NodeId:          "node1",
				Status:          &proto.HealthStatus{Status: proto.NodeStatus_NODE_HEALTHY},
				LastSeenVersion: 0,
			},
			expectErr:   true,
			expectedErr: assert.AnError,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mocks := &serverMocks{service: &MockService{}}
			tc.setupMocks(mocks)

			coordinator := NewCoordinator(mocks.service)
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

			mocks.assertExpectations(t)
		})
	}
}

func TestCoordinator_ListNodes(t *testing.T) {
	req := &proto.ListNodesRequest{}
	testCases := []struct {
		name         string
		setupMocks   func(*serverMocks)
		expectedResp *proto.ListNodesResponse
		expectErr    bool
		expectedErr  error
	}{
		{
			name: "success",
			setupMocks: func(mocks *serverMocks) {
				protoResp := &proto.ListNodesResponse{
					Nodes:          []*proto.NodeInfo{{Id: "node1", IpAddress: "localhost", Port: 9001, Status: proto.NodeStatus_NODE_HEALTHY, LastSeen: timestamppb.New(time.Unix(0, 0))}},
					CurrentVersion: 1,
				}
				mocks.service.On("listNodes", mock.Anything, mock.Anything).
					Return(common.ListNodesResponseFromProto(protoResp), nil).Once()
			},
			expectedResp: &proto.ListNodesResponse{
				Nodes:          []*proto.NodeInfo{{Id: "node1", IpAddress: "localhost", Port: 9001, Status: proto.NodeStatus_NODE_HEALTHY, LastSeen: timestamppb.New(time.Unix(0, 0))}},
				CurrentVersion: 1,
			},
			expectErr:   false,
			expectedErr: nil,
		},
		{
			name: "error: unexpected error",
			setupMocks: func(mocks *serverMocks) {
				mocks.service.On("listNodes", mock.Anything, mock.Anything).
					Return(common.ListNodesResponse{}, assert.AnError).Once()
			},
			expectErr:   true,
			expectedErr: assert.AnError,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mocks := &serverMocks{service: &MockService{}}
			tc.setupMocks(mocks)

			coordinator := NewCoordinator(mocks.service)
			resp, err := coordinator.ListNodes(context.Background(), req)

			if tc.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expectedResp, resp)
			}

			mocks.assertExpectations(t)
		})
	}
}
