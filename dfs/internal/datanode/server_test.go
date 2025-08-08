package datanode

import (
	"context"
	"testing"

	"github.com/mochivi/distributed-file-system/internal/apperr"
	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/internal/storage/chunk"
	"github.com/mochivi/distributed-file-system/pkg/proto"
	"github.com/mochivi/distributed-file-system/pkg/streaming"
	"github.com/mochivi/distributed-file-system/pkg/testutils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc/codes"
)

type serverMocks struct {
	service *MockService
}

func newServerMocks() *serverMocks {
	return &serverMocks{
		service: &MockService{},
	}
}

func (m *serverMocks) assertExpectations(t *testing.T) {
	m.service.AssertExpectations(t)
}

func TestDataNodeServer_PrepareChunkUpload(t *testing.T) {
	testCases := []struct {
		name                string
		setupMocks          func(*serverMocks)
		req                 *proto.UploadChunkRequest
		expectedResp        *proto.NodeReady
		expectErr           bool
		expectedSentinelErr error
		expectedCustomErr   error
	}{
		{
			name: "success",
			setupMocks: func(m *serverMocks) {
				m.service.On("prepareChunkUpload", mock.Anything, mock.Anything).
					Return(common.NodeReady{Accept: true, Message: "Ready to receive chunk data", SessionID: "session1"}, nil).Once()
			},
			req: &proto.UploadChunkRequest{
				ChunkHeader: &proto.ChunkHeader{Id: "chunk1", Size: 512},
			},
			expectedResp: &proto.NodeReady{
				Accept:    true,
				Message:   "Ready to receive chunk data",
				SessionId: "session1",
			},
			expectErr: false,
		},
		{
			name: "error: streaming session already exists",
			setupMocks: func(m *serverMocks) {
				m.service.On("prepareChunkUpload", mock.Anything, mock.Anything).
					Return(common.NodeReady{}, streaming.ErrSessionAlreadyExists).Once()
			},
			req: &proto.UploadChunkRequest{
				ChunkHeader: &proto.ChunkHeader{Id: "chunk1", Size: 512},
			},
			expectedResp:      nil,
			expectErr:         true,
			expectedCustomErr: apperr.Internal(streaming.ErrSessionAlreadyExists),
		},
		{
			name: "error: unexpected error",
			setupMocks: func(m *serverMocks) {
				m.service.On("prepareChunkUpload", mock.Anything, mock.Anything).
					Return(common.NodeReady{}, assert.AnError).Once()
			},
			req: &proto.UploadChunkRequest{
				ChunkHeader: &proto.ChunkHeader{Id: "chunk1", Size: 512},
			},
			expectedResp:        nil,
			expectErr:           true,
			expectedSentinelErr: assert.AnError,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mocks := newServerMocks()
			tc.setupMocks(mocks)
			server := NewDataNodeServer(mocks.service)

			resp, err := server.PrepareChunkUpload(context.Background(), tc.req)

			if tc.expectErr {
				assert.Error(t, err)
				if tc.expectedSentinelErr != nil {
					assert.ErrorIs(t, err, tc.expectedSentinelErr)
				}
				if tc.expectedCustomErr != nil {
					assert.ErrorAs(t, err, &tc.expectedCustomErr)
				}
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expectedResp.Accept, resp.Accept)
				assert.Equal(t, tc.expectedResp.Message, resp.Message)
				assert.Equal(t, tc.expectedResp.SessionId, resp.SessionId)
			}

			mocks.assertExpectations(t)
		})
	}
}

func TestDataNodeServer_PrepareChunkDownload(t *testing.T) {
	chunkHeader := common.ChunkHeader{ID: "chunk1", Size: 1024, Checksum: "abc"}

	testCases := []struct {
		name                string
		setupMocks          func(*serverMocks)
		req                 *proto.DownloadChunkRequest
		expectedResp        *proto.DownloadReady
		expectErr           bool
		expectedSentinelErr error
		expectedCustomErr   error
	}{
		{
			name: "success",
			setupMocks: func(m *serverMocks) {
				m.service.On("prepareChunkDownload", mock.Anything, mock.Anything).
					Return(common.DownloadReady{
						NodeReady:   common.NodeReady{Accept: true, Message: "Ready to download chunk data", SessionID: "session1"},
						ChunkHeader: chunkHeader,
					}, nil).Once()
			},
			req: &proto.DownloadChunkRequest{ChunkId: "chunk1"},
			expectedResp: &proto.DownloadReady{
				Ready:       &proto.NodeReady{Accept: true, Message: "Ready to download chunk data", SessionId: "session1"},
				ChunkHeader: chunkHeader.ToProto(),
			},
			expectErr: false,
		},
		{
			name: "error: invalid chunk ID",
			setupMocks: func(m *serverMocks) {
				m.service.On("prepareChunkDownload", mock.Anything, mock.Anything).
					Return(common.DownloadReady{}, chunk.ErrInvalidChunkID).Once()
			},
			req:               &proto.DownloadChunkRequest{ChunkId: "chunk1"},
			expectedResp:      nil,
			expectErr:         true,
			expectedCustomErr: apperr.InvalidArgument("invalid chunk ID", chunk.ErrInvalidChunkID),
		},
		{
			name: "error: streaming session already exists",
			setupMocks: func(m *serverMocks) {
				m.service.On("prepareChunkDownload", mock.Anything, mock.Anything).
					Return(common.DownloadReady{}, streaming.ErrSessionAlreadyExists).Once()
			},
			req:               &proto.DownloadChunkRequest{ChunkId: "chunk1"},
			expectedResp:      nil,
			expectErr:         true,
			expectedCustomErr: apperr.Internal(streaming.ErrSessionAlreadyExists),
		},
		{
			name: "error: unexpected error",
			setupMocks: func(m *serverMocks) {
				m.service.On("prepareChunkDownload", mock.Anything, mock.Anything).
					Return(common.DownloadReady{}, assert.AnError).Once()
			},
			req:                 &proto.DownloadChunkRequest{ChunkId: "chunk1"},
			expectedResp:        nil,
			expectErr:           true,
			expectedSentinelErr: assert.AnError,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mocks := newServerMocks()
			tc.setupMocks(mocks)

			server := NewDataNodeServer(mocks.service)

			resp, err := server.PrepareChunkDownload(context.Background(), tc.req)

			if tc.expectErr {
				assert.Error(t, err)
				if tc.expectedSentinelErr != nil {
					assert.ErrorIs(t, err, tc.expectedSentinelErr)
				}
				if tc.expectedCustomErr != nil {
					assert.ErrorAs(t, err, &tc.expectedCustomErr)
				}
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expectedResp.Ready.Accept, resp.Ready.Accept)
				assert.Equal(t, tc.expectedResp.Ready.SessionId, resp.Ready.SessionId)
				assert.Equal(t, tc.expectedResp.ChunkHeader.Id, resp.ChunkHeader.Id)
			}
			mocks.assertExpectations(t)
		})
	}
}

func TestDataNodeServer_DeleteChunk(t *testing.T) {
	testCases := []struct {
		name                string
		setupMocks          func(*serverMocks)
		req                 *proto.DeleteChunkRequest
		expectedResp        *proto.DeleteChunkResponse
		expectErr           bool
		expectedSentinelErr error
		expectedCustomErr   error
	}{
		{
			name: "success",
			setupMocks: func(m *serverMocks) {
				m.service.On("deleteChunk", mock.Anything, mock.Anything).
					Return(common.DeleteChunkResponse{Success: true, Message: "chunk deleted"}, nil).Once()
			},
			req:          &proto.DeleteChunkRequest{ChunkId: "chunk1"},
			expectedResp: &proto.DeleteChunkResponse{Success: true, Message: "chunk deleted"},
			expectErr:    false,
		},
		{
			name: "error: invalid chunk id",
			setupMocks: func(m *serverMocks) {
				m.service.On("deleteChunk", mock.Anything, mock.Anything).
					Return(common.DeleteChunkResponse{}, chunk.ErrInvalidChunkID).Once()
			},
			req:               &proto.DeleteChunkRequest{ChunkId: "chunk1"},
			expectedResp:      nil,
			expectErr:         true,
			expectedCustomErr: apperr.InvalidArgument("invalid chunk ID", chunk.ErrInvalidChunkID),
		},
		{
			name: "error: unexpected error",
			setupMocks: func(m *serverMocks) {
				m.service.On("deleteChunk", mock.Anything, mock.Anything).
					Return(common.DeleteChunkResponse{}, assert.AnError).Once()
			},
			req:                 &proto.DeleteChunkRequest{ChunkId: "chunk1"},
			expectedResp:        nil,
			expectErr:           true,
			expectedSentinelErr: assert.AnError,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mocks := newServerMocks()
			tc.setupMocks(mocks)

			server := NewDataNodeServer(mocks.service)

			resp, err := server.DeleteChunk(context.Background(), tc.req)

			if tc.expectErr {
				assert.Error(t, err)
				if tc.expectedSentinelErr != nil {
					assert.ErrorIs(t, err, tc.expectedSentinelErr)
				}
				if tc.expectedCustomErr != nil {
					assert.ErrorAs(t, err, &tc.expectedCustomErr)
				}
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expectedResp.Success, resp.Success)
				assert.Equal(t, tc.expectedResp.Message, resp.Message)
			}
			mocks.assertExpectations(t)
		})
	}
}

func TestDataNodeServer_BulkDeleteChunk(t *testing.T) {
	testCases := []struct {
		name                string
		setupMocks          func(*serverMocks)
		req                 *proto.BulkDeleteChunkRequest
		expectedResp        *proto.BulkDeleteChunkResponse
		expectErr           bool
		expectedSentinelErr error
		expectedCustomErr   error
	}{
		{
			name: "success",
			setupMocks: func(m *serverMocks) {
				m.service.On("bulkDeleteChunk", mock.Anything, mock.Anything).
					Return(common.BulkDeleteChunkResponse{Success: true, Message: "chunk deleted"}, nil).Once()
			},
			req:          &proto.BulkDeleteChunkRequest{ChunkIds: []string{"chunk1"}},
			expectedResp: &proto.BulkDeleteChunkResponse{Success: true, Message: "chunk deleted"},
			expectErr:    false,
		},
		{
			name: "error: unexpected error",
			setupMocks: func(m *serverMocks) {
				m.service.On("bulkDeleteChunk", mock.Anything, mock.Anything).
					Return(common.BulkDeleteChunkResponse{}, assert.AnError).Once()
			},
			req:                 &proto.BulkDeleteChunkRequest{ChunkIds: []string{"chunk1"}},
			expectedResp:        nil,
			expectErr:           true,
			expectedSentinelErr: assert.AnError,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mocks := newServerMocks()
			tc.setupMocks(mocks)

			server := NewDataNodeServer(mocks.service)

			resp, err := server.BulkDeleteChunk(context.Background(), tc.req)

			if tc.expectErr {
				assert.Error(t, err)
				if tc.expectedSentinelErr != nil {
					assert.ErrorIs(t, err, tc.expectedSentinelErr)
				}
				if tc.expectedCustomErr != nil {
					assert.ErrorAs(t, err, &tc.expectedCustomErr)
				}
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
	testCases := []struct {
		name                string
		setupMocks          func(*serverMocks)
		expectErr           bool
		expectedSentinelErr error
		expectedCustomErr   error
	}{
		{
			name: "success",
			setupMocks: func(m *serverMocks) {
				m.service.On("uploadChunkStream", mock.Anything, mock.Anything).
					Return(nil).Once()
			},
			expectErr: false,
		},
		{
			name: "error: session not found",
			setupMocks: func(m *serverMocks) {
				m.service.On("uploadChunkStream", mock.Anything, mock.Anything).
					Return(streaming.ErrSessionNotFound).Once()
			},
			expectErr:         true,
			expectedCustomErr: apperr.Wrap(codes.NotFound, "session not found", streaming.ErrSessionNotFound),
		},
		{
			name: "error: session expired",
			setupMocks: func(m *serverMocks) {
				m.service.On("uploadChunkStream", mock.Anything, mock.Anything).
					Return(streaming.ErrSessionExpired).Once()
			},
			expectErr:         true,
			expectedCustomErr: apperr.Wrap(codes.NotFound, "session expired", streaming.ErrSessionExpired),
		},
		{
			name: "error: checksum mismatch",
			setupMocks: func(m *serverMocks) {
				m.service.On("uploadChunkStream", mock.Anything, mock.Anything).
					Return(streaming.ErrChecksumMismatch).Once()
			},
			expectErr:         true,
			expectedCustomErr: apperr.Internal(streaming.ErrChecksumMismatch),
		},
		{
			name: "error: ack send failed",
			setupMocks: func(m *serverMocks) {
				m.service.On("uploadChunkStream", mock.Anything, mock.Anything).
					Return(streaming.ErrAckSendFailed).Once()
			},
			expectErr:         true,
			expectedCustomErr: apperr.Internal(streaming.ErrAckSendFailed),
		},
		{
			name: "error: unexpected error",
			setupMocks: func(m *serverMocks) {
				m.service.On("uploadChunkStream", mock.Anything, mock.Anything).
					Return(assert.AnError).Once()
			},
			expectErr:           true,
			expectedSentinelErr: assert.AnError,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mocks := newServerMocks()
			stream := &testutils.MockBidiStreamServer{}
			tc.setupMocks(mocks)

			server := NewDataNodeServer(mocks.service)
			err := server.UploadChunkStream(stream)

			if tc.expectErr {
				assert.Error(t, err)
				if tc.expectedSentinelErr != nil {
					assert.ErrorIs(t, err, tc.expectedSentinelErr)
				}
				if tc.expectedCustomErr != nil {
					assert.ErrorAs(t, err, &tc.expectedCustomErr)
				}
			} else {
				assert.NoError(t, err)
			}

			mocks.assertExpectations(t)
		})
	}
}

func TestDataNodeServer_DownloadChunkStream(t *testing.T) {
	req := &proto.DownloadStreamRequest{
		SessionId:       "session1",
		ChunkStreamSize: 1024,
	}
	testCases := []struct {
		name                string
		setupMocks          func(*serverMocks)
		expectErr           bool
		expectedSentinelErr error
		expectedCustomErr   error
	}{
		{
			name: "success",
			setupMocks: func(m *serverMocks) {
				m.service.On("downloadChunkStream", mock.Anything, mock.Anything).
					Return(nil).Once()
			},
			expectErr: false,
		},
		{
			name: "error: session not found",
			setupMocks: func(m *serverMocks) {
				m.service.On("downloadChunkStream", mock.Anything, mock.Anything).
					Return(streaming.ErrSessionNotFound).Once()
			},
			expectErr:         true,
			expectedCustomErr: apperr.Wrap(codes.NotFound, "session not found", streaming.ErrSessionNotFound),
		},
		{
			name: "error: session expired",
			setupMocks: func(m *serverMocks) {
				m.service.On("downloadChunkStream", mock.Anything, mock.Anything).
					Return(streaming.ErrSessionExpired).Once()
			},
			expectErr:         true,
			expectedCustomErr: apperr.Wrap(codes.NotFound, "session expired", streaming.ErrSessionExpired),
		},
		{
			name: "error: unexpected error",
			setupMocks: func(m *serverMocks) {
				m.service.On("downloadChunkStream", mock.Anything, mock.Anything).
					Return(assert.AnError).Once()
			},
			expectErr:           true,
			expectedSentinelErr: assert.AnError,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mocks := newServerMocks()
			stream := &testutils.MockStreamServer{}
			tc.setupMocks(mocks)

			server := NewDataNodeServer(mocks.service)
			err := server.DownloadChunkStream(req, stream)

			if tc.expectErr {
				assert.Error(t, err)
				if tc.expectedSentinelErr != nil {
					assert.ErrorIs(t, err, tc.expectedSentinelErr)
				}
				if tc.expectedCustomErr != nil {
					assert.ErrorAs(t, err, &tc.expectedCustomErr)
				}
			} else {
				assert.NoError(t, err)
			}

			mocks.assertExpectations(t)
		})
	}
}
