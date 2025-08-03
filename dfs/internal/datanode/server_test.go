package datanode

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/mochivi/distributed-file-system/internal/cluster"
	"github.com/mochivi/distributed-file-system/internal/cluster/state"
	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/internal/config"
	"github.com/mochivi/distributed-file-system/internal/storage/chunk"
	"github.com/mochivi/distributed-file-system/pkg/client_pool"
	"github.com/mochivi/distributed-file-system/pkg/logging"
	"github.com/mochivi/distributed-file-system/pkg/proto"
	"github.com/mochivi/distributed-file-system/pkg/streaming"
	"github.com/mochivi/distributed-file-system/pkg/testutils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type serverMocks struct {
	store             *chunk.MockChunkStore
	sessionManager    *streaming.MockStreamingSessionManager
	replication       *MockParalellReplicationService
	clusterViewer     *state.MockClusterStateManager
	coordinatorFinder *state.MockCoordinatorFinder
	selector          *cluster.MockNodeSelector
	serverStreamer    *streaming.MockServerStreamer
	clientPool        *client_pool.MockClientPool
}

func newServerMocks() *serverMocks {
	return &serverMocks{
		store:             &chunk.MockChunkStore{},
		sessionManager:    &streaming.MockStreamingSessionManager{},
		replication:       &MockParalellReplicationService{},
		clusterViewer:     &state.MockClusterStateManager{},
		coordinatorFinder: &state.MockCoordinatorFinder{},
		selector:          &cluster.MockNodeSelector{},
		serverStreamer:    &streaming.MockServerStreamer{},
		clientPool:        &client_pool.MockClientPool{},
	}
}

func (m *serverMocks) assertExpectations(t *testing.T) {
	m.store.AssertExpectations(t)
	m.sessionManager.AssertExpectations(t)
	m.replication.AssertExpectations(t)
	m.clusterViewer.AssertExpectations(t)
	m.coordinatorFinder.AssertExpectations(t)
	m.selector.AssertExpectations(t)
	m.serverStreamer.AssertExpectations(t)
	m.clientPool.AssertExpectations(t)
}

func TestDataNodeServer_PrepareChunkUpload(t *testing.T) {
	defaultInfo := &common.NodeInfo{ID: "test-node", Capacity: 2048, Used: 1024}
	defaultConfig := config.DataNodeConfig{StreamingSession: config.StreamingSessionManagerConfig{SessionTimeout: 1 * time.Minute}}
	logger := logging.NewTestLogger(slog.LevelError, true)

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
				session := streaming.NewStreamingSession(context.Background(), "session1", common.ChunkHeader{ID: "chunk1", Size: 10}, false)
				session.Status = streaming.SessionActive
				m.sessionManager.On("LoadByChunk", "chunk1").Return(nil, false)
				m.sessionManager.On("NewSession", mock.Anything, mock.AnythingOfType("common.ChunkHeader"), mock.AnythingOfType("bool")).Return(session)
				m.sessionManager.On("Store", session.SessionID, session).Return(nil)
			},
			req: &proto.UploadChunkRequest{
				ChunkHeader: &proto.ChunkHeader{Id: "chunk1", Size: 512},
			},
			expectedResp: &proto.NodeReady{Accept: true, Message: "Ready to receive chunk data"},
			expectErr:    false,
		},
		{
			name: "success: session already active",
			setupMocks: func(m *serverMocks) {
				activeSession := streaming.NewStreamingSession(context.Background(), "active-chunk", common.ChunkHeader{ID: "active-chunk", Size: 128}, false)
				m.sessionManager.On("LoadByChunk", "active-chunk").Return(activeSession, true)
			},
			req: &proto.UploadChunkRequest{ChunkHeader: &proto.ChunkHeader{Id: "active-chunk", Size: 128}},
			expectedResp: &proto.NodeReady{
				Accept:  true,
				Message: "Session already active",
			},
			expectErr: false,
		},
		{
			name: "success: session already completed",
			setupMocks: func(m *serverMocks) {
				session := streaming.NewStreamingSession(context.Background(), "session1", common.ChunkHeader{ID: "chunk1", Size: 10}, false)
				session.Status = streaming.SessionCompleted
				m.sessionManager.On("LoadByChunk", "chunk1").Return(session, true)
				m.sessionManager.On("Delete", session.SessionID)
			},
			req: &proto.UploadChunkRequest{
				ChunkHeader: &proto.ChunkHeader{Id: "chunk1", Size: 512},
			},
			expectedResp: &proto.NodeReady{Accept: false, Message: "Invalid session"},
			expectErr:    false,
		},
		{
			name: "success: session already completed",
			setupMocks: func(m *serverMocks) {
				session := streaming.NewStreamingSession(context.Background(), "session1", common.ChunkHeader{ID: "chunk1", Size: 10}, false)
				session.Status = streaming.SessionFailed
				m.sessionManager.On("LoadByChunk", "chunk1").Return(session, true)
				m.sessionManager.On("Delete", session.SessionID)
			},
			req: &proto.UploadChunkRequest{
				ChunkHeader: &proto.ChunkHeader{Id: "chunk1", Size: 512},
			},
			expectedResp: &proto.NodeReady{Accept: false, Message: "Invalid session"},
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

	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mocks := newServerMocks()
			tc.setupMocks(mocks)

			serverStreamerFactory := func(sessionManager streaming.SessionManager, config config.StreamerConfig) streaming.ServerStreamer {
				return mocks.serverStreamer
			}
			clientPoolFactory := func(nodes []*common.NodeInfo) (client_pool.ClientPool, error) {
				return mocks.clientPool, nil
			}
			container := NewContainer(mocks.store, mocks.replication, mocks.sessionManager, mocks.clusterViewer, mocks.coordinatorFinder, mocks.selector, serverStreamerFactory, clientPoolFactory)
			server := NewDataNodeServer(defaultInfo, defaultConfig, container, logger)

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
	logger := logging.NewTestLogger(slog.LevelError, true)
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
				session := streaming.NewStreamingSession(context.Background(), "session1", common.ChunkHeader{ID: "chunk1", Size: 1024}, false)
				m.store.On("Exists", "chunk1").Return(true)
				m.store.On("GetHeader", "chunk1").Return(chunkHeader, nil)
				m.sessionManager.On("NewSession", mock.Anything, mock.AnythingOfType("common.ChunkHeader"), mock.AnythingOfType("bool")).Return(session)
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
				m.store.On("GetHeader", "header-fail-chunk").Return(common.ChunkHeader{}, assert.AnError)
			},
			req:       &proto.DownloadChunkRequest{ChunkId: "header-fail-chunk"},
			expectErr: true,
		},
		{
			name: "error: streaming session already exists",
			setupMocks: func(m *serverMocks) {
				session := streaming.NewStreamingSession(context.Background(), "session1", common.ChunkHeader{ID: "existing-chunk", Size: 1024}, false)
				m.store.On("Exists", "existing-chunk").Return(true)
				m.store.On("GetHeader", "existing-chunk").Return(chunkHeader, nil)
				m.sessionManager.On("NewSession", mock.Anything, mock.AnythingOfType("common.ChunkHeader"), mock.AnythingOfType("bool")).Return(session)
				m.sessionManager.On("Store", session.SessionID, session).Return(assert.AnError)
			},
			req:       &proto.DownloadChunkRequest{ChunkId: "existing-chunk"},
			expectErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mocks := newServerMocks()
			tc.setupMocks(mocks)

			serverStreamerFactory := func(sessionManager streaming.SessionManager, config config.StreamerConfig) streaming.ServerStreamer {
				return mocks.serverStreamer
			}
			clientPoolFactory := func(nodes []*common.NodeInfo) (client_pool.ClientPool, error) {
				return mocks.clientPool, nil
			}
			container := NewContainer(mocks.store, mocks.replication, mocks.sessionManager, mocks.clusterViewer, mocks.coordinatorFinder, mocks.selector, serverStreamerFactory, clientPoolFactory)
			server := NewDataNodeServer(defaultInfo, defaultConfig, container, logger)

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
	logger := logging.NewTestLogger(slog.LevelError, true)

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
			mocks := newServerMocks()
			tc.setupMocks(mocks)

			serverStreamerFactory := func(sessionManager streaming.SessionManager, config config.StreamerConfig) streaming.ServerStreamer {
				return mocks.serverStreamer
			}
			clientPoolFactory := func(nodes []*common.NodeInfo) (client_pool.ClientPool, error) {
				return mocks.clientPool, nil
			}
			container := NewContainer(mocks.store, mocks.replication, mocks.sessionManager, mocks.clusterViewer, mocks.coordinatorFinder, mocks.selector, serverStreamerFactory, clientPoolFactory)
			server := NewDataNodeServer(defaultInfo, defaultConfig, container, logger)

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
	selfNode := &common.NodeInfo{ID: "test-node", Capacity: 2048, Used: 1024}
	defaultConfig := config.DataNodeConfig{StreamingSession: config.StreamingSessionManagerConfig{SessionTimeout: 1 * time.Minute}}
	logger := logging.NewTestLogger(slog.LevelError, true)

	testCases := []struct {
		name       string
		setupMocks func(*serverMocks, *testutils.MockBidiStreamServer)
		expectErr  bool
	}{
		{
			name: "success - single chunk upload",
			setupMocks: func(m *serverMocks, stream *testutils.MockBidiStreamServer) {
				chunkHeader := common.ChunkHeader{ID: "chunk1", Size: 10, Checksum: chunk.CalculateChecksum([]byte("test data!"))}
				session := streaming.NewStreamingSession(context.Background(), "session1", chunkHeader, false)
				session.Status = streaming.SessionActive

				m.serverStreamer.On("HandleFirstChunk", stream).Return(session, nil).Once()
				m.serverStreamer.On("ReceiveChunks", session, stream).Return([]byte("test data!"), nil).Once()
				m.store.On("Store", session.ChunkHeader, []byte("test data!")).Return(nil).Once()
				m.serverStreamer.On("SendFinalAck", session.SessionID, len([]byte("test data!")), stream).Return(nil).Once()
				m.sessionManager.On("Delete", session.SessionID).Return().Once()
			},
			expectErr: false,
		},
		{
			name: "success: multiple chunks upload",
			setupMocks: func(m *serverMocks, stream *testutils.MockBidiStreamServer) {
				chunkHeader := common.ChunkHeader{ID: "chunk2", Size: 10, Checksum: chunk.CalculateChecksum([]byte("test data!"))}
				session := streaming.NewStreamingSession(context.Background(), "session2", chunkHeader, false)
				session.Status = streaming.SessionActive

				m.serverStreamer.On("HandleFirstChunk", stream).Return(session, nil).Once()
				m.serverStreamer.On("ReceiveChunks", session, stream).Return([]byte("test data!"), nil).Once()
				m.store.On("Store", session.ChunkHeader, []byte("test data!")).Return(nil).Once()
				m.serverStreamer.On("SendFinalAck", session.SessionID, len([]byte("test data!")), stream).Return(nil).Once()
				m.sessionManager.On("Delete", session.SessionID).Return().Once()
			},
			expectErr: false,
		},
		{
			name: "success: upload with replication",
			setupMocks: func(m *serverMocks, stream *testutils.MockBidiStreamServer) {
				chunkHeader := common.ChunkHeader{ID: "chunk3", Size: 10, Checksum: chunk.CalculateChecksum([]byte("test data!"))}
				session := streaming.NewStreamingSession(context.Background(), "session3", chunkHeader, false)
				session.Status = streaming.SessionActive
				session.Propagate = true

				replicaNodes := []*common.NodeInfo{{ID: "node1"}, {ID: "node2"}}
				finalReplicaNodes := []*common.NodeInfo{{ID: "node1"}, {ID: "node2"}, selfNode}

				m.serverStreamer.On("HandleFirstChunk", stream).Return(session, nil).Once()
				m.serverStreamer.On("ReceiveChunks", session, stream).Return([]byte("test data!"), nil).Once()
				m.store.On("Store", session.ChunkHeader, []byte("test data!")).Return(nil).Once()
				m.serverStreamer.On("SendFinalAck", session.SessionID, len([]byte("test data!")), stream).Return(nil).Once()

				// inside replicate -- exclude self from selection
				m.selector.On("SelectBestNodes", N_NODES, selfNode).Return(replicaNodes, nil).Once()
				m.replication.On("Replicate", m.clientPool, session.ChunkHeader, []byte("test data!"), mock.Anything).Return(replicaNodes, nil).Once()
				m.clientPool.On("Close", mock.Anything).Once()

				// Stream will wait for the final ack response with replicas
				m.serverStreamer.On("SendFinalReplicasAck", session, finalReplicaNodes, stream).Return(nil).Once()
				m.sessionManager.On("Delete", session.SessionID).Once()
			},
			expectErr: false,
		},
		{
			name: "error: HandleFirstChunk fails",
			setupMocks: func(m *serverMocks, stream *testutils.MockBidiStreamServer) {
				m.serverStreamer.On("HandleFirstChunk", stream).Return(nil, assert.AnError)
			},
			expectErr: true,
		},
		{
			name: "error: ReceiveChunks fails",
			setupMocks: func(m *serverMocks, stream *testutils.MockBidiStreamServer) {
				chunkHeader := common.ChunkHeader{ID: "chunk4", Size: 4, Checksum: chunk.CalculateChecksum([]byte("test"))}
				session := streaming.NewStreamingSession(context.Background(), "session4", chunkHeader, false)
				session.Status = streaming.SessionActive

				m.serverStreamer.On("HandleFirstChunk", stream).Return(session, nil)
				m.serverStreamer.On("ReceiveChunks", session, stream).Return(nil, assert.AnError)
				m.sessionManager.On("Delete", session.SessionID).Return()
			},
			expectErr: true,
		},
		{
			name: "error: storage failure",
			setupMocks: func(m *serverMocks, stream *testutils.MockBidiStreamServer) {
				chunkHeader := common.ChunkHeader{ID: "chunk6", Size: 4, Checksum: chunk.CalculateChecksum([]byte("test"))}
				session := streaming.NewStreamingSession(context.Background(), "session6", chunkHeader, false)
				session.Status = streaming.SessionActive

				m.serverStreamer.On("HandleFirstChunk", stream).Return(session, nil)
				m.serverStreamer.On("ReceiveChunks", session, stream).Return([]byte("test"), nil)
				m.store.On("Store", session.ChunkHeader, []byte("test")).Return(assert.AnError)
				m.sessionManager.On("Delete", session.SessionID).Return()
			},
			expectErr: true,
		},
		{
			name: "error: replication failure",
			setupMocks: func(m *serverMocks, stream *testutils.MockBidiStreamServer) {
				chunkHeader := common.ChunkHeader{ID: "chunk7", Size: 4, Checksum: chunk.CalculateChecksum([]byte("test"))}
				session := streaming.NewStreamingSession(context.Background(), "session7", chunkHeader, false)
				session.Status = streaming.SessionActive
				session.Propagate = true

				m.serverStreamer.On("HandleFirstChunk", stream).Return(session, nil)
				m.serverStreamer.On("ReceiveChunks", session, stream).Return([]byte("test"), nil)
				m.store.On("Store", session.ChunkHeader, []byte("test")).Return(nil)
				m.serverStreamer.On("SendFinalAck", session.SessionID, len([]byte("test")), stream).Return(nil).Once()
				m.selector.On("SelectBestNodes", N_NODES, selfNode).Return([]*common.NodeInfo{{ID: "node-1"}}, nil).Once()
				m.replication.On("Replicate", m.clientPool, session.ChunkHeader, []byte("test"), mock.Anything).Return(nil, assert.AnError)
				m.sessionManager.On("Delete", session.SessionID).Return()
				m.clientPool.On("Close", mock.Anything).Return()
			},
			expectErr: true,
		},
		{
			name: "error: SendFinalReplicasAck fails",
			setupMocks: func(m *serverMocks, stream *testutils.MockBidiStreamServer) {
				chunkHeader := common.ChunkHeader{ID: "chunk8", Size: 4, Checksum: chunk.CalculateChecksum([]byte("test"))}
				session := streaming.NewStreamingSession(context.Background(), "session8", chunkHeader, false)
				session.Status = streaming.SessionActive
				session.Propagate = true

				replicaNodes := []*common.NodeInfo{{ID: "node1"}, {ID: "node2"}}
				finalReplicaNodes := []*common.NodeInfo{{ID: "node1"}, {ID: "node2"}, selfNode}

				m.serverStreamer.On("HandleFirstChunk", stream).Return(session, nil)
				m.serverStreamer.On("ReceiveChunks", session, stream).Return([]byte("test"), nil)
				m.store.On("Store", session.ChunkHeader, []byte("test")).Return(nil)
				m.serverStreamer.On("SendFinalAck", session.SessionID, len([]byte("test")), stream).Return(nil).Once()
				m.selector.On("SelectBestNodes", N_NODES, selfNode).Return(replicaNodes, nil).Once()
				m.replication.On("Replicate", m.clientPool, session.ChunkHeader, []byte("test"), mock.Anything).Return(replicaNodes, nil).Once()
				m.clientPool.On("Close", mock.Anything).Return().Once()
				m.serverStreamer.On("SendFinalReplicasAck", session, finalReplicaNodes, stream).Return(assert.AnError)
				m.sessionManager.On("Delete", session.SessionID).Return()
			},
			expectErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mocks := newServerMocks()
			stream := &testutils.MockBidiStreamServer{}
			tc.setupMocks(mocks, stream)

			serverStreamerFactory := func(sessionManager streaming.SessionManager, config config.StreamerConfig) streaming.ServerStreamer {
				return mocks.serverStreamer
			}
			clientPoolFactory := func(nodes []*common.NodeInfo) (client_pool.ClientPool, error) {
				return mocks.clientPool, nil
			}
			container := NewContainer(mocks.store, mocks.replication, mocks.sessionManager, mocks.clusterViewer,
				mocks.coordinatorFinder, mocks.selector, serverStreamerFactory, clientPoolFactory)

			server := NewDataNodeServer(selfNode, defaultConfig, container, logger)
			err := server.UploadChunkStream(stream)

			if tc.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			mocks.assertExpectations(t)
		})
	}
}

func TestDataNodeServer_DownloadChunkStream(t *testing.T) {
	defaultInfo := &common.NodeInfo{ID: "test-node"}
	defaultConfig := config.DataNodeConfig{StreamingSession: config.StreamingSessionManagerConfig{SessionTimeout: 1 * time.Minute}}
	logger := logging.NewTestLogger(slog.LevelError, true)

	testCases := []struct {
		name         string
		setupMocks   func(*serverMocks, *testutils.MockStreamServer)
		setupStreams func(*testutils.MockStreamServer)
		req          *proto.DownloadStreamRequest
		expectErr    bool
	}{
		{
			name: "success: download chunk",
			setupMocks: func(m *serverMocks, stream *testutils.MockStreamServer) {
				chunkHeader := common.ChunkHeader{ID: "chunk1", Size: 10}
				session := streaming.NewStreamingSession(context.Background(), "session1", chunkHeader, false)

				m.sessionManager.On("GetSession", mock.AnythingOfType("string")).Return(session, true)
				m.sessionManager.On("Delete", session.SessionID).Return()
				m.store.On("GetData", chunkHeader.ID).Return([]byte("test data!"), nil)
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
				chunkHeader := common.ChunkHeader{ID: "chunk2", Size: 20}
				session := streaming.NewStreamingSession(context.Background(), "session2", chunkHeader, false)

				m.sessionManager.On("GetSession", mock.AnythingOfType("string")).Return(session, true)
				m.sessionManager.On("Delete", session.SessionID).Return()
				m.store.On("GetData", chunkHeader.ID).Return([]byte("this is a test chunk"), nil)
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
				m.sessionManager.On("GetSession", mock.AnythingOfType("string")).Return(nil, false)
			},
			setupStreams: func(stream *testutils.MockStreamServer) {},
			req: &proto.DownloadStreamRequest{
				SessionId:       "invalid-session",
				ChunkStreamSize: 1024,
			},
			expectErr: true,
		},
		{
			name: "error: chunk data retrieval failure",
			setupMocks: func(m *serverMocks, stream *testutils.MockStreamServer) {
				chunkHeader := common.ChunkHeader{ID: "chunk3", Size: 10}
				session := streaming.NewStreamingSession(context.Background(), "session3", chunkHeader, false)

				m.sessionManager.On("GetSession", mock.AnythingOfType("string")).Return(session, true)
				m.sessionManager.On("Delete", session.SessionID).Return()
				m.store.On("GetData", chunkHeader.ID).Return(nil, assert.AnError)
			},
			setupStreams: func(stream *testutils.MockStreamServer) {},
			req: &proto.DownloadStreamRequest{
				SessionId:       "session3",
				ChunkStreamSize: 1024,
			},
			expectErr: true,
		},
		{
			name: "error: stream send failure",
			setupMocks: func(m *serverMocks, stream *testutils.MockStreamServer) {
				chunkHeader := common.ChunkHeader{ID: "chunk4", Size: 10}
				session := streaming.NewStreamingSession(context.Background(), "session4", chunkHeader, false)

				m.sessionManager.On("GetSession", mock.AnythingOfType("string")).Return(session, true)
				m.sessionManager.On("Delete", session.SessionID).Return()
				m.store.On("GetData", chunkHeader.ID).Return([]byte("test data!"), nil)
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
			mocks := newServerMocks()

			stream := &testutils.MockStreamServer{}
			tc.setupMocks(mocks, stream)
			tc.setupStreams(stream)

			serverStreamerFactory := func(sessionManager streaming.SessionManager, config config.StreamerConfig) streaming.ServerStreamer {
				return mocks.serverStreamer
			}
			clientPoolFactory := func(nodes []*common.NodeInfo) (client_pool.ClientPool, error) {
				return mocks.clientPool, nil
			}
			container := NewContainer(mocks.store, mocks.replication, mocks.sessionManager, mocks.clusterViewer, mocks.coordinatorFinder, mocks.selector, serverStreamerFactory, clientPoolFactory)
			server := NewDataNodeServer(defaultInfo, defaultConfig, container, logger)
			err := server.DownloadChunkStream(tc.req, stream)

			if tc.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			mocks.assertExpectations(t)
			stream.AssertExpectations(t)
		})
	}
}
