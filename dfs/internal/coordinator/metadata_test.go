package coordinator

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/internal/storage/metadata"
	"github.com/mochivi/distributed-file-system/pkg/logging"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestTrackUpload(t *testing.T) {
	logger := logging.NewTestLogger(slog.LevelDebug, true)
	manager := NewMetadataSessionManager(5*time.Second, logger)

	req := common.UploadRequest{
		Path:     "/test/file.txt",
		Size:     1024,
		Checksum: "checksum",
	}
	numChunks := 3
	sessionID := "test-session"

	manager.trackUpload(sessionID, req, numChunks)

	// Use reflection or a test helper to inspect internal state if necessary,
	// but for this case we can test the outcome via the commit method.
	// We'll check if the session exists by attempting to commit.
	mockStore := new(metadata.MockMetadataStore)
	chunkInfos := make([]common.ChunkInfo, numChunks)
	// We expect the commit to fail on the metastore call, but not because the session is missing
	mockStore.On("PutFile", req.Path, mock.Anything).Return(nil)

	err := manager.commit(context.Background(), sessionID, chunkInfos, mockStore)
	assert.NoError(t, err, "Commit should not fail because session is missing")
}

func TestCommit(t *testing.T) {
	testCases := []struct {
		name          string
		sessionID     string
		commitTimeout time.Duration
		setupManager  func(*metadataSessionManager, string)
		setupMocks    func(*metadata.MockMetadataStore)
		expectErr     bool
		testChunkInfo []common.ChunkInfo
	}{
		{
			name:          "success",
			sessionID:     "test-session-success",
			commitTimeout: 5 * time.Second,
			setupManager: func(m *metadataSessionManager, sid string) {
				m.trackUpload(sid, common.UploadRequest{Path: "/test/success.txt"}, 1)
			},
			setupMocks: func(ms *metadata.MockMetadataStore) {
				ms.On("PutFile", "/test/success.txt", mock.AnythingOfType("*common.FileInfo")).Return(nil).Once()
			},
			expectErr:     false,
			testChunkInfo: []common.ChunkInfo{{Replicas: []*common.NodeInfo{{ID: "node1"}}}},
		},
		{
			name:          "error: session not found",
			sessionID:     "non-existent-session",
			commitTimeout: 5 * time.Second,
			setupManager:  func(m *metadataSessionManager, sid string) {},
			setupMocks:    func(ms *metadata.MockMetadataStore) {},
			expectErr:     true,
		},
		{
			name:          "error: session expired",
			sessionID:     "test-session-expired",
			commitTimeout: 1 * time.Millisecond,
			setupManager: func(m *metadataSessionManager, sid string) {
				m.trackUpload(sid, common.UploadRequest{Path: "/test/expired.txt"}, 1)
				time.Sleep(2 * time.Millisecond) // Ensure session expires
			},
			setupMocks: func(ms *metadata.MockMetadataStore) {},
			expectErr:  true,
		},
		{
			name:          "error: metadata store failure",
			sessionID:     "test-session-store-failure",
			commitTimeout: 5 * time.Second,
			setupManager: func(m *metadataSessionManager, sid string) {
				m.trackUpload(sid, common.UploadRequest{Path: "/test/store_failure.txt"}, 1)
			},
			setupMocks: func(ms *metadata.MockMetadataStore) {
				ms.On("PutFile", "/test/store_failure.txt", mock.AnythingOfType("*common.FileInfo")).Return(assert.AnError).Once()
			},
			expectErr:     true,
			testChunkInfo: []common.ChunkInfo{{Replicas: []*common.NodeInfo{{ID: "node1"}}}},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			logger := logging.NewTestLogger(slog.LevelDebug, true)
			manager := NewMetadataSessionManager(tc.commitTimeout, logger)
			mockStore := new(metadata.MockMetadataStore)

			tc.setupManager(manager, tc.sessionID)
			tc.setupMocks(mockStore)

			err := manager.commit(context.Background(), tc.sessionID, tc.testChunkInfo, mockStore)

			if tc.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			mockStore.AssertExpectations(t)
		})
	}
}
