package datanode

import (
	"context"
	"testing"
	"time"

	"github.com/mochivi/distributed-file-system/internal/cluster/state"
	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/internal/config"
	"github.com/mochivi/distributed-file-system/internal/storage/chunk"
	"github.com/mochivi/distributed-file-system/pkg/client_pool"
	"github.com/mochivi/distributed-file-system/pkg/streaming"
	"github.com/mochivi/distributed-file-system/pkg/testutils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type serviceMocks struct {
	store             *chunk.MockChunkStore
	sessionManager    *streaming.MockStreamingSessionManager
	replication       *MockParalellReplicationService
	clusterViewer     *state.MockClusterStateManager
	coordinatorFinder *state.MockCoordinatorFinder
	selector          *state.MockNodeSelector
	serverStreamer    *streaming.MockServerStreamer
	clientPool        *client_pool.MockClientPool
}

func newServiceMocks() *serviceMocks {
	return &serviceMocks{
		store:             &chunk.MockChunkStore{},
		sessionManager:    &streaming.MockStreamingSessionManager{},
		replication:       &MockParalellReplicationService{},
		clusterViewer:     &state.MockClusterStateManager{},
		coordinatorFinder: &state.MockCoordinatorFinder{},
		selector:          &state.MockNodeSelector{},
		serverStreamer:    &streaming.MockServerStreamer{},
		clientPool:        &client_pool.MockClientPool{},
	}
}

func (m *serviceMocks) assertExpectations(t *testing.T) {
	m.store.AssertExpectations(t)
	m.sessionManager.AssertExpectations(t)
	m.replication.AssertExpectations(t)
	m.clusterViewer.AssertExpectations(t)
	m.coordinatorFinder.AssertExpectations(t)
	m.selector.AssertExpectations(t)
	m.serverStreamer.AssertExpectations(t)
	m.clientPool.AssertExpectations(t)
}

func TestDataNodeService_PrepareChunkUpload(t *testing.T) {
	defaultInfo := &common.NodeInfo{ID: "test-node", Capacity: 2048, Used: 1024}
	defaultConfig := config.DataNodeConfig{StreamingSession: config.StreamingSessionManagerConfig{SessionTimeout: 1 * time.Minute}}

	testCases := []struct {
		name         string
		setupMocks   func(*serviceMocks)
		req          common.UploadChunkRequest
		expectedResp common.NodeReady
		expectErr    bool
	}{
		{
			name: "success",
			setupMocks: func(m *serviceMocks) {
				session := streaming.NewStreamingSession(context.Background(), "session1", common.ChunkHeader{ID: "chunk1", Size: 10}, false)
				session.Status = streaming.SessionActive
				m.sessionManager.On("LoadByChunk", "chunk1").Return(nil, streaming.ErrSessionNotFound)
				m.sessionManager.On("NewSession", mock.Anything, mock.AnythingOfType("common.ChunkHeader"), mock.AnythingOfType("bool")).Return(session)
				m.sessionManager.On("Store", session.SessionID, session).Return(nil)
			},
			req: common.UploadChunkRequest{
				ChunkHeader: common.ChunkHeader{ID: "chunk1", Size: 512},
			},
			expectedResp: common.NodeReady{Accept: true, Message: "Ready to receive chunk data", SessionID: "session1"},
			expectErr:    false,
		},
		{
			name: "success: session already active",
			setupMocks: func(m *serviceMocks) {
				activeSession := streaming.NewStreamingSession(context.Background(), "active-chunk", common.ChunkHeader{ID: "active-chunk", Size: 128}, false)
				m.sessionManager.On("LoadByChunk", "active-chunk").Return(activeSession, nil)
			},
			req: common.UploadChunkRequest{ChunkHeader: common.ChunkHeader{ID: "active-chunk", Size: 128}},
			expectedResp: common.NodeReady{
				Accept:  true,
				Message: "Session already active",
			},
			expectErr: false,
		},
		{
			name: "success: session already completed",
			setupMocks: func(m *serviceMocks) {
				session := streaming.NewStreamingSession(context.Background(), "session1", common.ChunkHeader{ID: "chunk1", Size: 10}, false)
				session.Status = streaming.SessionCompleted
				m.sessionManager.On("LoadByChunk", "chunk1").Return(session, nil)
				m.sessionManager.On("Delete", session.SessionID)
			},
			req: common.UploadChunkRequest{
				ChunkHeader: common.ChunkHeader{ID: "chunk1", Size: 512},
			},
			expectedResp: common.NodeReady{Accept: false, Message: "Invalid session"},
			expectErr:    false,
		},
		{
			name: "success: session already failed",
			setupMocks: func(m *serviceMocks) {
				session := streaming.NewStreamingSession(context.Background(), "session1", common.ChunkHeader{ID: "chunk1", Size: 10}, false)
				session.Status = streaming.SessionFailed
				m.sessionManager.On("LoadByChunk", "chunk1").Return(session, nil)
				m.sessionManager.On("Delete", session.SessionID)
			},
			req: common.UploadChunkRequest{
				ChunkHeader: common.ChunkHeader{ID: "chunk1", Size: 512},
			},
			expectedResp: common.NodeReady{Accept: false, Message: "Invalid session"},
			expectErr:    false,
		},
		{
			name:       "error: invalid chunk header size",
			req:        common.UploadChunkRequest{ChunkHeader: common.ChunkHeader{ID: "chunk1", Size: 0}},
			setupMocks: func(m *serviceMocks) {},
			expectedResp: common.NodeReady{
				Accept:  false,
				Message: "Invalid chunk metadata",
			},
			expectErr: false,
		},
		{
			name:       "error: invalid chunk header id",
			req:        common.UploadChunkRequest{ChunkHeader: common.ChunkHeader{ID: "", Size: 512}},
			setupMocks: func(m *serviceMocks) {},
			expectedResp: common.NodeReady{
				Accept:  false,
				Message: "Invalid chunk metadata",
			},
			expectErr: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mocks := newServiceMocks()
			tc.setupMocks(mocks)

			serverStreamerFactory := func(sessionManager streaming.SessionManager, config config.StreamerConfig) streaming.ServerStreamer {
				return mocks.serverStreamer
			}
			clientPoolFactory := func(nodes []*common.NodeInfo) (client_pool.ClientPool, error) {
				return mocks.clientPool, nil
			}
			container := NewContainer(mocks.store, mocks.replication, mocks.sessionManager, mocks.clusterViewer, mocks.coordinatorFinder, mocks.selector, serverStreamerFactory, clientPoolFactory)
			service := NewService(defaultConfig, defaultInfo, container)

			resp, err := service.prepareChunkUpload(context.Background(), tc.req)

			if tc.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expectedResp.Accept, resp.Accept)
				assert.Equal(t, tc.expectedResp.Message, resp.Message)
				if tc.expectedResp.Accept {
					assert.NotEmpty(t, resp.SessionID)
				}
			}

			mocks.assertExpectations(t)
		})
	}
}

func TestDataNodeService_PrepareChunkDownload(t *testing.T) {
	defaultInfo := &common.NodeInfo{ID: "test-node"}
	defaultConfig := config.DataNodeConfig{StreamingSession: config.StreamingSessionManagerConfig{SessionTimeout: 1 * time.Minute}}
	chunkHeader := common.ChunkHeader{ID: "chunk1", Size: 1024, Checksum: "abc"}

	testCases := []struct {
		name         string
		setupMocks   func(*serviceMocks)
		req          common.DownloadChunkRequest
		expectedResp common.DownloadReady
		expectErr    bool
	}{
		{
			name: "success",
			setupMocks: func(m *serviceMocks) {
				session := streaming.NewStreamingSession(context.Background(), "session1", common.ChunkHeader{ID: "chunk1", Size: 1024}, false)
				m.store.On("Exists", mock.Anything, "chunk1").Return(nil)
				m.store.On("GetHeader", mock.Anything, "chunk1").Return(chunkHeader, nil)
				m.sessionManager.On("NewSession", mock.Anything, mock.AnythingOfType("common.ChunkHeader"), mock.AnythingOfType("bool")).Return(session)
				m.sessionManager.On("Store", session.SessionID, session).Return(nil)
			},
			req: common.DownloadChunkRequest{ChunkID: "chunk1"},
			expectedResp: common.DownloadReady{
				NodeReady:   common.NodeReady{Accept: true, Message: "Ready to download chunk data", SessionID: "session1"},
				ChunkHeader: chunkHeader,
			},
			expectErr: false,
		},
		{
			name: "error: chunk not found",
			setupMocks: func(m *serviceMocks) {
				m.store.On("Exists", mock.Anything, "not-found-chunk").Return(assert.AnError)
			},
			req:       common.DownloadChunkRequest{ChunkID: "not-found-chunk"},
			expectErr: true,
		},
		{
			name: "error: failed to get chunk header",
			setupMocks: func(m *serviceMocks) {
				m.store.On("Exists", mock.Anything, "header-fail-chunk").Return(nil)
				m.store.On("GetHeader", mock.Anything, "header-fail-chunk").Return(common.ChunkHeader{}, assert.AnError)
			},
			req:       common.DownloadChunkRequest{ChunkID: "header-fail-chunk"},
			expectErr: true,
		},
		{
			name: "error: streaming session already exists",
			setupMocks: func(m *serviceMocks) {
				session := streaming.NewStreamingSession(context.Background(), "session1", common.ChunkHeader{ID: "existing-chunk", Size: 1024}, false)
				m.store.On("Exists", mock.Anything, "existing-chunk").Return(nil)
				m.store.On("GetHeader", mock.Anything, "existing-chunk").Return(chunkHeader, nil)
				m.sessionManager.On("NewSession", mock.Anything, mock.AnythingOfType("common.ChunkHeader"), mock.AnythingOfType("bool")).Return(session)
				m.sessionManager.On("Store", session.SessionID, session).Return(streaming.ErrSessionAlreadyExists)
			},
			req:       common.DownloadChunkRequest{ChunkID: "existing-chunk"},
			expectErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mocks := newServiceMocks()
			tc.setupMocks(mocks)

			serverStreamerFactory := func(sessionManager streaming.SessionManager, config config.StreamerConfig) streaming.ServerStreamer {
				return mocks.serverStreamer
			}
			clientPoolFactory := func(nodes []*common.NodeInfo) (client_pool.ClientPool, error) {
				return mocks.clientPool, nil
			}
			container := NewContainer(mocks.store, mocks.replication, mocks.sessionManager, mocks.clusterViewer, mocks.coordinatorFinder, mocks.selector, serverStreamerFactory, clientPoolFactory)
			service := NewService(defaultConfig, defaultInfo, container)

			resp, err := service.prepareChunkDownload(context.Background(), tc.req)

			if tc.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.True(t, resp.Accept)
				assert.NotEmpty(t, resp.SessionID)
				assert.Equal(t, tc.expectedResp.ChunkHeader.ID, resp.ChunkHeader.ID)
			}
			mocks.assertExpectations(t)
		})
	}
}

func TestDataNodeService_DeleteChunk(t *testing.T) {
	defaultInfo := &common.NodeInfo{ID: "test-node"}
	defaultConfig := config.DataNodeConfig{}

	testCases := []struct {
		name         string
		setupMocks   func(*serviceMocks)
		req          common.DeleteChunkRequest
		expectedResp common.DeleteChunkResponse
		expectErr    bool
	}{
		{
			name: "success",
			setupMocks: func(m *serviceMocks) {
				m.store.On("Exists", mock.Anything, "chunk1").Return(nil)
				m.store.On("Delete", mock.Anything, "chunk1").Return(nil)
			},
			req:          common.DeleteChunkRequest{ChunkID: "chunk1"},
			expectedResp: common.DeleteChunkResponse{Success: true, Message: "Chunk deleted"},
			expectErr:    false,
		},
		{
			name: "error: chunk not found",
			setupMocks: func(m *serviceMocks) {
				m.store.On("Exists", mock.Anything, "not-found-chunk").Return(assert.AnError)
			},
			req:          common.DeleteChunkRequest{ChunkID: "not-found-chunk"},
			expectedResp: common.DeleteChunkResponse{},
			expectErr:    true,
		},
		{
			name: "error: failed to delete",
			setupMocks: func(m *serviceMocks) {
				m.store.On("Exists", mock.Anything, "delete-fail-chunk").Return(nil)
				m.store.On("Delete", mock.Anything, "delete-fail-chunk").Return(assert.AnError)
			},
			req:       common.DeleteChunkRequest{ChunkID: "delete-fail-chunk"},
			expectErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mocks := newServiceMocks()
			tc.setupMocks(mocks)

			serverStreamerFactory := func(sessionManager streaming.SessionManager, config config.StreamerConfig) streaming.ServerStreamer {
				return mocks.serverStreamer
			}
			clientPoolFactory := func(nodes []*common.NodeInfo) (client_pool.ClientPool, error) {
				return mocks.clientPool, nil
			}
			container := NewContainer(mocks.store, mocks.replication, mocks.sessionManager, mocks.clusterViewer, mocks.coordinatorFinder, mocks.selector, serverStreamerFactory, clientPoolFactory)
			service := NewService(defaultConfig, defaultInfo, container)

			resp, err := service.deleteChunk(context.Background(), tc.req)

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

func TestDataNodeService_UploadChunkStream(t *testing.T) {
	selfNode := &common.NodeInfo{ID: "test-node", Capacity: 2048, Used: 1024}
	defaultConfig := config.DataNodeConfig{StreamingSession: config.StreamingSessionManagerConfig{SessionTimeout: 1 * time.Minute}}

	testCases := []struct {
		name       string
		setupMocks func(*serviceMocks, *testutils.MockBidiStreamServer)
		expectErr  bool
	}{
		{
			name: "success - single chunk upload",
			setupMocks: func(m *serviceMocks, stream *testutils.MockBidiStreamServer) {
				chunkHeader := common.ChunkHeader{ID: "chunk1", Size: 10, Checksum: chunk.CalculateChecksum([]byte("test data!"))}
				session := streaming.NewStreamingSession(context.Background(), "session1", chunkHeader, false)
				session.Status = streaming.SessionActive

				m.serverStreamer.On("HandleFirstChunkFrame", stream).Return(session, nil).Once()
				m.serverStreamer.On("ReceiveChunkFrames", session, stream).Return([]byte("test data!"), nil).Once()
				m.store.On("Store", mock.Anything, session.ChunkHeader, []byte("test data!")).Return(nil).Once()
				m.serverStreamer.On("SendFinalAck", session.SessionID, len([]byte("test data!")), stream).Return(nil).Once()
				m.sessionManager.On("Delete", session.SessionID).Return().Once()
			},
			expectErr: false,
		},
		{
			name: "success: multiple chunks upload",
			setupMocks: func(m *serviceMocks, stream *testutils.MockBidiStreamServer) {
				chunkHeader := common.ChunkHeader{ID: "chunk2", Size: 10, Checksum: chunk.CalculateChecksum([]byte("test data!"))}
				session := streaming.NewStreamingSession(context.Background(), "session2", chunkHeader, false)
				session.Status = streaming.SessionActive

				m.serverStreamer.On("HandleFirstChunkFrame", stream).Return(session, nil).Once()
				m.serverStreamer.On("ReceiveChunkFrames", session, stream).Return([]byte("test data!"), nil).Once()
				m.store.On("Store", mock.Anything, session.ChunkHeader, []byte("test data!")).Return(nil).Once()
				m.serverStreamer.On("SendFinalAck", session.SessionID, len([]byte("test data!")), stream).Return(nil).Once()
				m.sessionManager.On("Delete", session.SessionID).Return().Once()
			},
			expectErr: false,
		},
		{
			name: "success: upload with replication",
			setupMocks: func(m *serviceMocks, stream *testutils.MockBidiStreamServer) {
				chunkHeader := common.ChunkHeader{ID: "chunk3", Size: 10, Checksum: chunk.CalculateChecksum([]byte("test data!"))}
				session := streaming.NewStreamingSession(context.Background(), "session3", chunkHeader, false)
				session.Status = streaming.SessionActive
				session.Propagate = true

				replicaNodes := []*common.NodeInfo{{ID: "node1"}, {ID: "node2"}}
				finalReplicaNodes := []*common.NodeInfo{{ID: "node1"}, {ID: "node2"}, selfNode}

				m.serverStreamer.On("HandleFirstChunkFrame", stream).Return(session, nil).Once()
				m.serverStreamer.On("ReceiveChunkFrames", session, stream).Return([]byte("test data!"), nil).Once()
				m.store.On("Store", mock.Anything, session.ChunkHeader, []byte("test data!")).Return(nil).Once()
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
			setupMocks: func(m *serviceMocks, stream *testutils.MockBidiStreamServer) {
				m.serverStreamer.On("HandleFirstChunkFrame", stream).Return(nil, assert.AnError)
			},
			expectErr: true,
		},
		{
			name: "error: ReceiveChunkFrames fails",
			setupMocks: func(m *serviceMocks, stream *testutils.MockBidiStreamServer) {
				chunkHeader := common.ChunkHeader{ID: "chunk4", Size: 4, Checksum: chunk.CalculateChecksum([]byte("test"))}
				session := streaming.NewStreamingSession(context.Background(), "session4", chunkHeader, false)
				session.Status = streaming.SessionActive

				m.serverStreamer.On("HandleFirstChunkFrame", stream).Return(session, nil)
				m.serverStreamer.On("ReceiveChunkFrames", session, stream).Return(nil, assert.AnError)
				m.sessionManager.On("Delete", session.SessionID).Return()
			},
			expectErr: true,
		},
		{
			name: "error: storage failure",
			setupMocks: func(m *serviceMocks, stream *testutils.MockBidiStreamServer) {
				chunkHeader := common.ChunkHeader{ID: "chunk6", Size: 4, Checksum: chunk.CalculateChecksum([]byte("test"))}
				session := streaming.NewStreamingSession(context.Background(), "session6", chunkHeader, false)
				session.Status = streaming.SessionActive

				m.serverStreamer.On("HandleFirstChunkFrame", stream).Return(session, nil)
				m.serverStreamer.On("ReceiveChunkFrames", session, stream).Return([]byte("test"), nil)
				m.store.On("Store", mock.Anything, session.ChunkHeader, []byte("test")).Return(assert.AnError)
				m.sessionManager.On("Delete", session.SessionID).Return()
			},
			expectErr: true,
		},
		{
			name: "error: replication failure",
			setupMocks: func(m *serviceMocks, stream *testutils.MockBidiStreamServer) {
				chunkHeader := common.ChunkHeader{ID: "chunk7", Size: 4, Checksum: chunk.CalculateChecksum([]byte("test"))}
				session := streaming.NewStreamingSession(context.Background(), "session7", chunkHeader, false)
				session.Status = streaming.SessionActive
				session.Propagate = true

				m.serverStreamer.On("HandleFirstChunkFrame", stream).Return(session, nil)
				m.serverStreamer.On("ReceiveChunkFrames", session, stream).Return([]byte("test"), nil)
				m.store.On("Store", mock.Anything, session.ChunkHeader, []byte("test")).Return(nil)
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
			setupMocks: func(m *serviceMocks, stream *testutils.MockBidiStreamServer) {
				chunkHeader := common.ChunkHeader{ID: "chunk8", Size: 4, Checksum: chunk.CalculateChecksum([]byte("test"))}
				session := streaming.NewStreamingSession(context.Background(), "session8", chunkHeader, false)
				session.Status = streaming.SessionActive
				session.Propagate = true

				replicaNodes := []*common.NodeInfo{{ID: "node1"}, {ID: "node2"}}
				finalReplicaNodes := []*common.NodeInfo{{ID: "node1"}, {ID: "node2"}, selfNode}

				m.serverStreamer.On("HandleFirstChunkFrame", stream).Return(session, nil)
				m.serverStreamer.On("ReceiveChunkFrames", session, stream).Return([]byte("test"), nil)
				m.store.On("Store", mock.Anything, session.ChunkHeader, []byte("test")).Return(nil)
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
			mocks := newServiceMocks()
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

			service := NewService(defaultConfig, selfNode, container)
			err := service.uploadChunkStream(stream)

			if tc.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			mocks.assertExpectations(t)
		})
	}
}

func TestDataNodeService_DownloadChunkStream(t *testing.T) {
	defaultInfo := &common.NodeInfo{ID: "test-node"}
	defaultConfig := config.DataNodeConfig{StreamingSession: config.StreamingSessionManagerConfig{SessionTimeout: 1 * time.Minute}}

	testCases := []struct {
		name         string
		setupMocks   func(*serviceMocks, *testutils.MockStreamServer)
		setupStreams func(*testutils.MockStreamServer)
		req          common.DownloadStreamRequest
		expectErr    bool
	}{
		{
			name: "success: download chunk",
			setupMocks: func(m *serviceMocks, stream *testutils.MockStreamServer) {
				chunkHeader := common.ChunkHeader{ID: "chunk1", Size: 10}
				session := streaming.NewStreamingSession(context.Background(), "session1", chunkHeader, false)

				m.sessionManager.On("GetSession", mock.AnythingOfType("string")).Return(session, nil)
				m.sessionManager.On("Delete", session.SessionID).Return()
				m.store.On("GetData", mock.Anything, chunkHeader.ID).Return([]byte("test data!"), nil)
			},
			setupStreams: func(stream *testutils.MockStreamServer) {
				stream.On("Send", mock.AnythingOfType("*proto.ChunkDataStream")).Return(nil).Once()
			},
			req: common.DownloadStreamRequest{
				SessionID:       "session1",
				ChunkStreamSize: 1024,
			},
			expectErr: false,
		},
		{
			name: "success: download large chunk in multiple frames",
			setupMocks: func(m *serviceMocks, stream *testutils.MockStreamServer) {
				chunkHeader := common.ChunkHeader{ID: "chunk2", Size: 20}
				session := streaming.NewStreamingSession(context.Background(), "session2", chunkHeader, false)

				m.sessionManager.On("GetSession", mock.AnythingOfType("string")).Return(session, nil)
				m.sessionManager.On("Delete", session.SessionID).Return()
				m.store.On("GetData", mock.Anything, chunkHeader.ID).Return([]byte("this is a test chunk"), nil)
			},
			setupStreams: func(stream *testutils.MockStreamServer) {
				stream.On("Send", mock.AnythingOfType("*proto.ChunkDataStream")).Return(nil).Times(2)
			},
			req: common.DownloadStreamRequest{
				SessionID:       "session2",
				ChunkStreamSize: 10, // Small buffer to force multiple sends
			},
			expectErr: false,
		},
		{
			name: "error: session not found",
			setupMocks: func(m *serviceMocks, stream *testutils.MockStreamServer) {
				m.sessionManager.On("GetSession", mock.AnythingOfType("string")).Return(nil, streaming.ErrSessionNotFound)
			},
			setupStreams: func(stream *testutils.MockStreamServer) {},
			req: common.DownloadStreamRequest{
				SessionID:       "not-found-session",
				ChunkStreamSize: 1024,
			},
			expectErr: true,
		},
		{
			name: "error: session expired",
			setupMocks: func(m *serviceMocks, stream *testutils.MockStreamServer) {
				m.sessionManager.On("GetSession", mock.AnythingOfType("string")).Return(nil, streaming.ErrSessionExpired)
			},
			setupStreams: func(stream *testutils.MockStreamServer) {},
			req: common.DownloadStreamRequest{
				SessionID:       "expired-session",
				ChunkStreamSize: 1024,
			},
			expectErr: true,
		},
		{
			name: "error: chunk data retrieval failure",
			setupMocks: func(m *serviceMocks, stream *testutils.MockStreamServer) {
				chunkHeader := common.ChunkHeader{ID: "chunk3", Size: 10}
				session := streaming.NewStreamingSession(context.Background(), "session3", chunkHeader, false)

				m.sessionManager.On("GetSession", mock.AnythingOfType("string")).Return(session, nil)
				m.sessionManager.On("Delete", session.SessionID).Return()
				m.store.On("GetData", mock.Anything, chunkHeader.ID).Return(nil, assert.AnError)
			},
			setupStreams: func(stream *testutils.MockStreamServer) {},
			req: common.DownloadStreamRequest{
				SessionID:       "session3",
				ChunkStreamSize: 1024,
			},
			expectErr: true,
		},
		{
			name: "error: stream send failure",
			setupMocks: func(m *serviceMocks, stream *testutils.MockStreamServer) {
				chunkHeader := common.ChunkHeader{ID: "chunk4", Size: 10}
				session := streaming.NewStreamingSession(context.Background(), "session4", chunkHeader, false)

				m.sessionManager.On("GetSession", mock.AnythingOfType("string")).Return(session, nil)
				m.sessionManager.On("Delete", session.SessionID).Return()
				m.store.On("GetData", mock.Anything, chunkHeader.ID).Return([]byte("test data!"), nil)
			},
			setupStreams: func(stream *testutils.MockStreamServer) {
				stream.On("Send", mock.AnythingOfType("*proto.ChunkDataStream")).Return(assert.AnError)
			},
			req: common.DownloadStreamRequest{
				SessionID:       "session4",
				ChunkStreamSize: 1024,
			},
			expectErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mocks := newServiceMocks()

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
			service := NewService(defaultConfig, defaultInfo, container)
			err := service.downloadChunkStream(tc.req, stream)

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
