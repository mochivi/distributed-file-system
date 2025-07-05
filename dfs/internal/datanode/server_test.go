package datanode

import (
	"context"
	"crypto/sha256"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/mochivi/distributed-file-system/internal/cluster"
	"github.com/mochivi/distributed-file-system/internal/cluster/state"
	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/internal/config"
	"github.com/mochivi/distributed-file-system/internal/storage/chunk"
	"github.com/mochivi/distributed-file-system/pkg/logging"
	"github.com/mochivi/distributed-file-system/pkg/proto"
	"github.com/mochivi/distributed-file-system/pkg/testutils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type serverMocks struct {
	store             *chunk.MockChunkStore
	sessionManager    *MockStreamingSessionManager
	replication       *MockParalellReplicationService
	clusterViewer     *state.MockClusterStateManager
	coordinatorFinder *state.MockCoordinatorFinder
	selector          *cluster.MockNodeSelector
}

func (m *serverMocks) assertExpectations(t *testing.T) {
	m.store.AssertExpectations(t)
	m.sessionManager.AssertExpectations(t)
	m.replication.AssertExpectations(t)
	m.clusterViewer.AssertExpectations(t)
	m.coordinatorFinder.AssertExpectations(t)
	m.selector.AssertExpectations(t)
}

func TestDataNodeServer_PrepareChunkUpload(t *testing.T) {
	defaultInfo := &common.NodeInfo{ID: "test-node", Capacity: 2048, Used: 1024}
	defaultConfig := config.DataNodeConfig{StreamingSession: config.StreamingSessionManagerConfig{SessionTimeout: 1 * time.Minute}}
	logger := logging.NewTestLogger(slog.LevelError)

	testCases := []struct {
		name         string
		setupMocks   func(*serverMocks)
		req          *proto.UploadChunkRequest
		expectedResp *proto.NodeReady
		expectErr    bool
	}{
		{
			name: "success",
			setupMocks: func(m *serverMocks) {
				session := &StreamingSession{SessionID: "session1", Status: SessionActive, ExpiresAt: time.Now().Add(1 * time.Minute), logger: logger}
				m.sessionManager.On("LoadByChunk", "chunk1").Return(nil, false)
				m.sessionManager.On("NewSession", mock.AnythingOfType("common.ChunkHeader"), mock.AnythingOfType("bool")).Return(session)
				m.sessionManager.On("Store", session.SessionID, session).Return(nil)
			},
			req: &proto.UploadChunkRequest{
				ChunkHeader: &proto.ChunkHeader{Id: "chunk1", Size: 512},
			},
			expectedResp: &proto.NodeReady{Accept: true, Message: "Ready to receive chunk data"},
			expectErr:    false,
		},
		{
			name:       "error: invalid chunk header size",
			req:        &proto.UploadChunkRequest{ChunkHeader: &proto.ChunkHeader{Id: "chunk1", Size: 0}},
			setupMocks: func(m *serverMocks) {},
			expectedResp: &proto.NodeReady{
				Accept:  false,
				Message: "Invalid chunk metadata",
			},
			expectErr: false,
		},
		{
			name:       "error: invalid chunk header id",
			req:        &proto.UploadChunkRequest{ChunkHeader: &proto.ChunkHeader{Id: "", Size: 512}},
			setupMocks: func(m *serverMocks) {},
			expectedResp: &proto.NodeReady{
				Accept:  false,
				Message: "Invalid chunk metadata",
			},
			expectErr: false,
		},
		// Test has to wait until resource checking for node is implemented
		// {
		// 	name:       "error: insufficient capacity",
		// 	req:        &proto.UploadChunkRequest{ChunkHeader: &proto.ChunkHeader{Id: "chunk-too-big", Size: 2048}},
		// 	setupMocks: func(m *serverMocks) {},
		// 	expectedResp: &proto.NodeReady{
		// 		Accept:  false,
		// 		Message: "Insufficient storage capacity",
		// 	},
		// 	expectErr: false,
		// },
		{
			name: "error: session already active",
			setupMocks: func(m *serverMocks) {
				activeSession := &StreamingSession{Status: SessionActive}
				m.sessionManager.On("LoadByChunk", "active-chunk").Return(activeSession, true)
			},
			req: &proto.UploadChunkRequest{ChunkHeader: &proto.ChunkHeader{Id: "active-chunk", Size: 128}},
			expectedResp: &proto.NodeReady{
				Accept:  false,
				Message: "session still active",
			},
			expectErr: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mocks := &serverMocks{
				store:             new(chunk.MockChunkStore),
				sessionManager:    new(MockStreamingSessionManager),
				replication:       new(MockParalellReplicationService),
				clusterViewer:     new(state.MockClusterStateManager),
				coordinatorFinder: new(state.MockCoordinatorFinder),
				selector:          new(cluster.MockNodeSelector),
			}
			tc.setupMocks(mocks)

			server := NewDataNodeServer(defaultInfo, defaultConfig, mocks.store, mocks.replication, mocks.sessionManager, mocks.clusterViewer, mocks.coordinatorFinder, mocks.selector, logger)

			resp, err := server.PrepareChunkUpload(context.Background(), tc.req)

			if tc.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expectedResp.Accept, resp.Accept)
				assert.Equal(t, tc.expectedResp.Message, resp.Message)
				if tc.expectedResp.Accept {
					assert.NotEmpty(t, resp.SessionId)
				}
			}

			mocks.assertExpectations(t)
		})
	}
}

func TestDataNodeServer_PrepareChunkDownload(t *testing.T) {
	defaultInfo := &common.NodeInfo{ID: "test-node"}
	defaultConfig := config.DataNodeConfig{StreamingSession: config.StreamingSessionManagerConfig{SessionTimeout: 1 * time.Minute}}
	logger := logging.NewTestLogger(slog.LevelError)
	chunkHeader := common.ChunkHeader{ID: "chunk1", Size: 1024, Checksum: "abc"}

	testCases := []struct {
		name         string
		setupMocks   func(*serverMocks)
		req          *proto.DownloadChunkRequest
		expectedResp *proto.DownloadReady
		expectErr    bool
	}{
		{
			name: "success",
			setupMocks: func(m *serverMocks) {
				session := &StreamingSession{SessionID: "session1", Status: SessionActive, ExpiresAt: time.Now().Add(1 * time.Minute), logger: logger}
				m.store.On("Exists", "chunk1").Return(true)
				m.store.On("GetChunkHeader", "chunk1").Return(chunkHeader, nil)
				m.sessionManager.On("NewSession", mock.AnythingOfType("common.ChunkHeader"), mock.AnythingOfType("bool")).Return(session)
				m.sessionManager.On("Store", session.SessionID, session).Return(nil)
			},
			req: &proto.DownloadChunkRequest{ChunkId: "chunk1"},
			expectedResp: &proto.DownloadReady{
				Ready:       &proto.NodeReady{Accept: true, Message: "Ready to download chunk data"},
				ChunkHeader: chunkHeader.ToProto(),
			},
			expectErr: false,
		},
		{
			name: "error: chunk not found",
			setupMocks: func(m *serverMocks) {
				m.store.On("Exists", "not-found-chunk").Return(false)
			},
			req:       &proto.DownloadChunkRequest{ChunkId: "not-found-chunk"},
			expectErr: true,
		},
		{
			name: "error: failed to get chunk header",
			setupMocks: func(m *serverMocks) {
				m.store.On("Exists", "header-fail-chunk").Return(true)
				m.store.On("GetChunkHeader", "header-fail-chunk").Return(common.ChunkHeader{}, assert.AnError)
			},
			req:       &proto.DownloadChunkRequest{ChunkId: "header-fail-chunk"},
			expectErr: true,
		},
		{
			name: "error: streaming session already exists",
			setupMocks: func(m *serverMocks) {
				session := &StreamingSession{SessionID: "session1", Status: SessionActive, ExpiresAt: time.Now().Add(1 * time.Minute), logger: logger}
				m.store.On("Exists", "existing-chunk").Return(true)
				m.store.On("GetChunkHeader", "existing-chunk").Return(chunkHeader, nil)
				m.sessionManager.On("NewSession", mock.AnythingOfType("common.ChunkHeader"), mock.AnythingOfType("bool")).Return(session)
				m.sessionManager.On("Store", session.SessionID, session).Return(assert.AnError)
			},
			req:       &proto.DownloadChunkRequest{ChunkId: "existing-chunk"},
			expectErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mocks := &serverMocks{
				store:             new(chunk.MockChunkStore),
				sessionManager:    new(MockStreamingSessionManager),
				replication:       new(MockParalellReplicationService),
				clusterViewer:     new(state.MockClusterStateManager),
				coordinatorFinder: new(state.MockCoordinatorFinder),
				selector:          new(cluster.MockNodeSelector),
			}
			tc.setupMocks(mocks)

			server := NewDataNodeServer(defaultInfo, defaultConfig, mocks.store, mocks.replication, mocks.sessionManager, mocks.clusterViewer, mocks.coordinatorFinder, mocks.selector, logger)
			resp, err := server.PrepareChunkDownload(context.Background(), tc.req)

			if tc.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.True(t, resp.Ready.Accept)
				assert.NotEmpty(t, resp.Ready.SessionId)
				assert.Equal(t, tc.expectedResp.ChunkHeader.Id, resp.ChunkHeader.Id)
			}
			mocks.assertExpectations(t)
		})
	}
}

func TestDataNodeServer_DeleteChunk(t *testing.T) {
	defaultInfo := &common.NodeInfo{ID: "test-node"}
	defaultConfig := config.DataNodeConfig{}
	logger := logging.NewTestLogger(slog.LevelError)

	testCases := []struct {
		name         string
		setupMocks   func(*serverMocks)
		req          *proto.DeleteChunkRequest
		expectedResp *proto.DeleteChunkResponse
		expectErr    bool
	}{
		{
			name: "success",
			setupMocks: func(m *serverMocks) {
				m.store.On("Exists", "chunk1").Return(true)
				m.store.On("Delete", "chunk1").Return(nil)
			},
			req:          &proto.DeleteChunkRequest{ChunkId: "chunk1"},
			expectedResp: &proto.DeleteChunkResponse{Success: true, Message: "chunk deleted"},
			expectErr:    false,
		},
		{
			name: "error: chunk not found",
			setupMocks: func(m *serverMocks) {
				m.store.On("Exists", "not-found-chunk").Return(false)
			},
			req:          &proto.DeleteChunkRequest{ChunkId: "not-found-chunk"},
			expectedResp: &proto.DeleteChunkResponse{Success: false, Message: "chunk not found"},
			expectErr:    false,
		},
		{
			name: "error: failed to delete",
			setupMocks: func(m *serverMocks) {
				m.store.On("Exists", "delete-fail-chunk").Return(true)
				m.store.On("Delete", "delete-fail-chunk").Return(assert.AnError)
			},
			req:       &proto.DeleteChunkRequest{ChunkId: "delete-fail-chunk"},
			expectErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mocks := &serverMocks{
				store:             new(chunk.MockChunkStore),
				sessionManager:    new(MockStreamingSessionManager),
				replication:       new(MockParalellReplicationService),
				clusterViewer:     new(state.MockClusterStateManager),
				coordinatorFinder: new(state.MockCoordinatorFinder),
				selector:          new(cluster.MockNodeSelector),
			}
			tc.setupMocks(mocks)

			server := NewDataNodeServer(defaultInfo, defaultConfig, mocks.store, mocks.replication, mocks.sessionManager, mocks.clusterViewer, mocks.coordinatorFinder, mocks.selector, logger)
			resp, err := server.DeleteChunk(context.Background(), tc.req)

			if tc.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expectedResp.Success, resp.Success)
				assert.Equal(t, tc.expectedResp.Message, resp.Message)
			}
			mocks.assertExpectations(t)
		})
	}
}

func TestDataNodeServer_UploadChunkStream(t *testing.T) {
	defaultInfo := &common.NodeInfo{ID: "test-node", Capacity: 2048, Used: 1024}
	defaultConfig := config.DataNodeConfig{StreamingSession: config.StreamingSessionManagerConfig{SessionTimeout: 1 * time.Minute}}
	logger := logging.NewTestLogger(slog.LevelError)

	testCases := []struct {
		name            string
		setupMocks      func(*serverMocks, *testutils.MockBidiStreamServer)
		setupStreams    func(*testutils.MockBidiStreamServer)
		expectErr       bool
		expectedErrCode codes.Code
	}{
		{
			name: "success - single chunk upload",
			setupMocks: func(m *serverMocks, stream *testutils.MockBidiStreamServer) {
				session := &StreamingSession{
					SessionID: "session1",
					ChunkHeader: common.ChunkHeader{
						ID:       "chunk1",
						Size:     10,
						Checksum: chunk.CalculateChecksum([]byte("test data!")),
					},
					Propagate:       false,
					Status:          SessionActive,
					RunningChecksum: sha256.New(),
					ExpiresAt:       time.Now().Add(1 * time.Minute),
					logger:          logger,
				}
				m.sessionManager.On("Load", "session1").Return(session, true)
				m.sessionManager.On("Delete", "session1").Return()
				m.store.On("Store", session.ChunkHeader, []byte("test data!")).Return(nil)
			},
			setupStreams: func(stream *testutils.MockBidiStreamServer) {
				chunk := &proto.ChunkDataStream{
					SessionId: "session1",
					ChunkId:   "chunk1",
					Data:      []byte("test data!"),
					Offset:    0,
					IsFinal:   true,
				}
				stream.On("Recv").Return(chunk, nil).Once()
				stream.On("Recv").Return(nil, io.EOF).Once()
				stream.On("Send", mock.AnythingOfType("*proto.ChunkDataAck")).Return(nil).Twice()
			},
			expectErr: false,
		},
		{
			name: "success: multiple chunks upload",
			setupMocks: func(m *serverMocks, stream *testutils.MockBidiStreamServer) {
				session := &StreamingSession{
					SessionID: "session2",
					ChunkHeader: common.ChunkHeader{
						ID:       "chunk2",
						Size:     10,
						Checksum: chunk.CalculateChecksum([]byte("test data!")),
					},
					Propagate:       false,
					Status:          SessionActive,
					RunningChecksum: sha256.New(),
					ExpiresAt:       time.Now().Add(1 * time.Minute),
					logger:          logger,
				}
				m.sessionManager.On("Load", "session2").Return(session, true)
				m.sessionManager.On("Delete", "session2").Return()
				m.store.On("Store", session.ChunkHeader, []byte("test data!")).Return(nil)
			},
			setupStreams: func(stream *testutils.MockBidiStreamServer) {
				chunk1 := &proto.ChunkDataStream{
					SessionId: "session2",
					ChunkId:   "chunk2",
					Data:      []byte("test "),
					Offset:    0,
					IsFinal:   false,
				}
				chunk2 := &proto.ChunkDataStream{
					SessionId: "session2",
					ChunkId:   "chunk2",
					Data:      []byte("data!"),
					Offset:    5,
					IsFinal:   true,
				}
				stream.On("Recv").Return(chunk1, nil).Once()
				stream.On("Recv").Return(chunk2, nil).Once()
				stream.On("Recv").Return(nil, io.EOF).Once()
				stream.On("Send", mock.AnythingOfType("*proto.ChunkDataAck")).Return(nil).Times(3)
			},
			expectErr: false,
		},
		{
			name: "success: upload with replication",
			setupMocks: func(m *serverMocks, stream *testutils.MockBidiStreamServer) {
				session := &StreamingSession{
					SessionID: "session3",
					ChunkHeader: common.ChunkHeader{
						ID:       "chunk3",
						Size:     10,
						Checksum: chunk.CalculateChecksum([]byte("test data!")),
					},
					Propagate:       true,
					Status:          SessionActive,
					RunningChecksum: sha256.New(),
					ExpiresAt:       time.Now().Add(1 * time.Minute),
					logger:          logger,
				}
				nodes := []*common.NodeInfo{{ID: "node1"}, {ID: "node2"}}
				m.sessionManager.On("Load", "session3").Return(session, true)
				m.sessionManager.On("Delete", "session3").Return()
				m.store.On("Store", session.ChunkHeader, []byte("test data!")).Return(nil)
				m.selector.On("SelectBestNodes", N_NODES).Return(nodes, true)
				m.replication.On("Replicate", mock.Anything, session.ChunkHeader, []byte("test data!"), N_REPLICAS-1).Return(nodes, nil)
			},
			setupStreams: func(stream *testutils.MockBidiStreamServer) {
				chunk := &proto.ChunkDataStream{
					SessionId: "session3",
					ChunkId:   "chunk3",
					Data:      []byte("test data!"),
					Offset:    0,
					IsFinal:   true,
				}
				stream.On("Recv").Return(chunk, nil).Once()
				stream.On("Recv").Return(nil, io.EOF).Once()
				stream.On("Send", mock.AnythingOfType("*proto.ChunkDataAck")).Return(nil).Times(3)
			},
			expectErr: false,
		},
		{
			name: "error: invalid session",
			setupMocks: func(m *serverMocks, stream *testutils.MockBidiStreamServer) {
				m.sessionManager.On("Load", "invalid-session").Return(nil, false)
			},
			setupStreams: func(stream *testutils.MockBidiStreamServer) {
				chunk := &proto.ChunkDataStream{
					SessionId: "invalid-session",
					ChunkId:   "chunk1",
					Data:      []byte("test"),
					Offset:    0,
					IsFinal:   true,
				}
				stream.On("Recv").Return(chunk, nil).Once()
			},
			expectErr:       true,
			expectedErrCode: codes.NotFound,
		},
		{
			name: "error: data out of order",
			setupMocks: func(m *serverMocks, stream *testutils.MockBidiStreamServer) {
				session := &StreamingSession{
					SessionID:       "session4",
					ChunkHeader:     common.ChunkHeader{ID: "chunk4", Size: 10, Checksum: "abc"},
					Propagate:       false,
					Status:          SessionActive,
					RunningChecksum: sha256.New(),
					ExpiresAt:       time.Now().Add(1 * time.Minute),
					logger:          logger,
				}
				m.sessionManager.On("Load", "session4").Return(session, true)
				m.sessionManager.On("Delete", "session4").Return()
			},
			setupStreams: func(stream *testutils.MockBidiStreamServer) {
				chunk := &proto.ChunkDataStream{
					SessionId: "session4",
					ChunkId:   "chunk4",
					Data:      []byte("test"),
					Offset:    5, // Should be 0 for first chunk
					IsFinal:   true,
				}
				stream.On("Recv").Return(chunk, nil).Once()
			},
			expectErr:       true,
			expectedErrCode: codes.Internal,
		},
		{
			name: "error: checksum mismatch",
			setupMocks: func(m *serverMocks, stream *testutils.MockBidiStreamServer) {
				session := &StreamingSession{
					SessionID:       "session5",
					ChunkHeader:     common.ChunkHeader{ID: "chunk5", Size: 4, Checksum: "wrongchecksum"},
					Propagate:       false,
					Status:          SessionActive,
					RunningChecksum: sha256.New(),
					ExpiresAt:       time.Now().Add(1 * time.Minute),
					logger:          logger,
				}
				m.sessionManager.On("Load", "session5").Return(session, true)
				m.sessionManager.On("Delete", "session5").Return()
			},
			setupStreams: func(stream *testutils.MockBidiStreamServer) {
				chunk := &proto.ChunkDataStream{
					SessionId: "session5",
					ChunkId:   "chunk5",
					Data:      []byte("test"),
					Offset:    0,
					IsFinal:   true,
				}
				stream.On("Recv").Return(chunk, nil).Once()
				stream.On("Send", mock.AnythingOfType("*proto.ChunkDataAck")).Return(nil).Once()
			},
			expectErr:       true,
			expectedErrCode: codes.Internal,
		},
		{
			name: "error: storage failure",
			setupMocks: func(m *serverMocks, stream *testutils.MockBidiStreamServer) {
				session := &StreamingSession{
					SessionID: "session6",
					ChunkHeader: common.ChunkHeader{
						ID:       "chunk6",
						Size:     4,
						Checksum: chunk.CalculateChecksum([]byte("test")),
					},
					Propagate:       false,
					Status:          SessionActive,
					RunningChecksum: sha256.New(),
					ExpiresAt:       time.Now().Add(1 * time.Minute),
					logger:          logger,
				}
				m.sessionManager.On("Load", "session6").Return(session, true)
				m.sessionManager.On("Delete", "session6").Return()
				m.store.On("Store", session.ChunkHeader, []byte("test")).Return(assert.AnError)
			},
			setupStreams: func(stream *testutils.MockBidiStreamServer) {
				chunk := &proto.ChunkDataStream{
					SessionId: "session6",
					ChunkId:   "chunk6",
					Data:      []byte("test"),
					Offset:    0,
					IsFinal:   true,
				}
				stream.On("Recv").Return(chunk, nil).Once()
				stream.On("Send", mock.AnythingOfType("*proto.ChunkDataAck")).Return(nil).Once()
			},
			expectErr:       true,
			expectedErrCode: codes.Internal,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mocks := &serverMocks{
				store:             new(chunk.MockChunkStore),
				sessionManager:    new(MockStreamingSessionManager),
				replication:       new(MockParalellReplicationService),
				clusterViewer:     new(state.MockClusterStateManager),
				coordinatorFinder: new(state.MockCoordinatorFinder),
				selector:          new(cluster.MockNodeSelector),
			}

			stream := new(testutils.MockBidiStreamServer)
			tc.setupMocks(mocks, stream)
			tc.setupStreams(stream)

			server := NewDataNodeServer(defaultInfo, defaultConfig, mocks.store, mocks.replication, mocks.sessionManager, mocks.clusterViewer, mocks.coordinatorFinder, mocks.selector, logger)
			err := server.UploadChunkStream(stream)

			if tc.expectErr {
				assert.Error(t, err)
				if tc.expectedErrCode != codes.OK {
					st, ok := status.FromError(err)
					assert.True(t, ok)
					assert.Equal(t, tc.expectedErrCode, st.Code())
				}
			} else {
				assert.NoError(t, err)
			}

			mocks.assertExpectations(t)
			stream.AssertExpectations(t)
		})
	}
}

func TestDataNodeServer_DownloadChunkStream(t *testing.T) {
	defaultInfo := &common.NodeInfo{ID: "test-node"}
	defaultConfig := config.DataNodeConfig{StreamingSession: config.StreamingSessionManagerConfig{SessionTimeout: 1 * time.Minute}}
	logger := logging.NewTestLogger(slog.LevelError)

	testCases := []struct {
		name            string
		setupMocks      func(*serverMocks, *testutils.MockStreamServer)
		setupStreams    func(*testutils.MockStreamServer)
		req             *proto.DownloadStreamRequest
		expectErr       bool
		expectedErrCode codes.Code
	}{
		{
			name: "success: download chunk",
			setupMocks: func(m *serverMocks, stream *testutils.MockStreamServer) {
				session := &StreamingSession{
					SessionID:   "session1",
					ChunkHeader: common.ChunkHeader{ID: "chunk1", Size: 10},
					Status:      SessionActive,
					ExpiresAt:   time.Now().Add(1 * time.Minute),
					logger:      logger,
				}
				m.sessionManager.On("Load", "session1").Return(session, true)
				m.sessionManager.On("Delete", "session1").Return()
				m.store.On("GetChunkData", "chunk1").Return([]byte("test data!"), nil)
			},
			setupStreams: func(stream *testutils.MockStreamServer) {
				stream.On("Send", mock.AnythingOfType("*proto.ChunkDataStream")).Return(nil).Once()
			},
			req: &proto.DownloadStreamRequest{
				SessionId:       "session1",
				ChunkStreamSize: 1024,
			},
			expectErr: false,
		},
		{
			name: "success: download large chunk in multiple frames",
			setupMocks: func(m *serverMocks, stream *testutils.MockStreamServer) {
				session := &StreamingSession{
					SessionID:   "session2",
					ChunkHeader: common.ChunkHeader{ID: "chunk2", Size: 20},
					Status:      SessionActive,
					ExpiresAt:   time.Now().Add(1 * time.Minute),
					logger:      logger,
				}
				m.sessionManager.On("Load", "session2").Return(session, true)
				m.sessionManager.On("Delete", "session2").Return()
				m.store.On("GetChunkData", "chunk2").Return([]byte("this is a test chunk"), nil)
			},
			setupStreams: func(stream *testutils.MockStreamServer) {
				stream.On("Send", mock.AnythingOfType("*proto.ChunkDataStream")).Return(nil).Times(2)
			},
			req: &proto.DownloadStreamRequest{
				SessionId:       "session2",
				ChunkStreamSize: 10, // Small buffer to force multiple sends
			},
			expectErr: false,
		},
		{
			name: "error: invalid session",
			setupMocks: func(m *serverMocks, stream *testutils.MockStreamServer) {
				m.sessionManager.On("Load", "invalid-session").Return(nil, false)
			},
			setupStreams: func(stream *testutils.MockStreamServer) {},
			req: &proto.DownloadStreamRequest{
				SessionId:       "invalid-session",
				ChunkStreamSize: 1024,
			},
			expectErr:       true,
			expectedErrCode: codes.NotFound,
		},
		{
			name: "error: chunk data retrieval failure",
			setupMocks: func(m *serverMocks, stream *testutils.MockStreamServer) {
				session := &StreamingSession{
					SessionID:   "session3",
					ChunkHeader: common.ChunkHeader{ID: "chunk3", Size: 10},
					Status:      SessionActive,
					ExpiresAt:   time.Now().Add(1 * time.Minute),
					logger:      logger,
				}
				m.sessionManager.On("Load", "session3").Return(session, true)
				m.sessionManager.On("Delete", "session3").Return()
				m.store.On("GetChunkData", "chunk3").Return(nil, assert.AnError)
			},
			setupStreams: func(stream *testutils.MockStreamServer) {},
			req: &proto.DownloadStreamRequest{
				SessionId:       "session3",
				ChunkStreamSize: 1024,
			},
			expectErr:       true,
			expectedErrCode: codes.Internal,
		},
		{
			name: "error: stream send failure",
			setupMocks: func(m *serverMocks, stream *testutils.MockStreamServer) {
				session := &StreamingSession{
					SessionID:   "session4",
					ChunkHeader: common.ChunkHeader{ID: "chunk4", Size: 10},
					Status:      SessionActive,
					ExpiresAt:   time.Now().Add(1 * time.Minute),
					logger:      logger,
				}
				m.sessionManager.On("Load", "session4").Return(session, true)
				m.sessionManager.On("Delete", "session4").Return()
				m.store.On("GetChunkData", "chunk4").Return([]byte("test data!"), nil)
			},
			setupStreams: func(stream *testutils.MockStreamServer) {
				stream.On("Send", mock.AnythingOfType("*proto.ChunkDataStream")).Return(assert.AnError)
			},
			req: &proto.DownloadStreamRequest{
				SessionId:       "session4",
				ChunkStreamSize: 1024,
			},
			expectErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mocks := &serverMocks{
				store:             new(chunk.MockChunkStore),
				sessionManager:    new(MockStreamingSessionManager),
				replication:       new(MockParalellReplicationService),
				clusterViewer:     new(state.MockClusterStateManager),
				coordinatorFinder: new(state.MockCoordinatorFinder),
				selector:          new(cluster.MockNodeSelector),
			}

			stream := new(testutils.MockStreamServer)
			tc.setupMocks(mocks, stream)
			tc.setupStreams(stream)

			server := NewDataNodeServer(defaultInfo, defaultConfig, mocks.store, mocks.replication, mocks.sessionManager, mocks.clusterViewer, mocks.coordinatorFinder, mocks.selector, logger)
			err := server.DownloadChunkStream(tc.req, stream)

			if tc.expectErr {
				assert.Error(t, err)
				if tc.expectedErrCode != codes.OK {
					st, ok := status.FromError(err)
					assert.True(t, ok)
					assert.Equal(t, tc.expectedErrCode, st.Code())
				}
			} else {
				assert.NoError(t, err)
			}

			mocks.assertExpectations(t)
			stream.AssertExpectations(t)
		})
	}
}
