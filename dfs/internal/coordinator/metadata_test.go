package coordinator

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/internal/storage/chunk"
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
	chunkIDs := chunk.FormatChunkIDs(chunk.HashFilepath(req.Path), 3)
	sessionID := common.MetadataSessionID("test-session")

	manager.trackUpload(common.MetadataSessionID(sessionID), req, chunkIDs)

	// Use reflection or a test helper to inspect internal state if necessary,
	// but for this case we can test the outcome via the commit method.
	// We'll check if the session exists by attempting to commit.
	mockStore := new(metadata.MockMetadataStore)
	chunkInfos := make([]common.ChunkInfo, len(chunkIDs))
	// We expect the commit to fail on the metastore call, but not because the session is missing
	mockStore.On("PutFile", mock.Anything, req.Path, mock.Anything).Return(nil)

	err := manager.commit(context.Background(), sessionID, chunkInfos, mockStore)
	assert.NoError(t, err, "Commit should not fail because session is missing")
}

func TestCommit(t *testing.T) {
	testCases := []struct {
		name          string
		sessionID     common.MetadataSessionID
		commitTimeout time.Duration
		setupManager  func(*metadataSessionManager, common.MetadataSessionID)
		setupMocks    func(*metadata.MockMetadataStore)
		expectErr     bool
		testChunkInfo []common.ChunkInfo
	}{
		{
			name:          "success",
			sessionID:     common.MetadataSessionID("test-session-success"),
			commitTimeout: 5 * time.Second,
			setupManager: func(m *metadataSessionManager, sid common.MetadataSessionID) {
				m.trackUpload(sid, common.UploadRequest{Path: "/test/success.txt"}, []string{"chunk1"})
			},
			setupMocks: func(ms *metadata.MockMetadataStore) {
				ms.On("PutFile", mock.Anything, "/test/success.txt", mock.AnythingOfType("*common.FileInfo")).Return(nil).Once()
			},
			expectErr:     false,
			testChunkInfo: []common.ChunkInfo{{Replicas: []*common.NodeInfo{{ID: "node1"}}}},
		},
		{
			name:          "error: session not found",
			sessionID:     common.MetadataSessionID("non-existent-session"),
			commitTimeout: 5 * time.Second,
			setupManager:  func(m *metadataSessionManager, sid common.MetadataSessionID) {},
			setupMocks:    func(ms *metadata.MockMetadataStore) {},
			expectErr:     true,
		},
		{
			name:          "error: session expired",
			sessionID:     common.MetadataSessionID("test-session-expired"),
			commitTimeout: 1 * time.Millisecond,
			setupManager: func(m *metadataSessionManager, sid common.MetadataSessionID) {
				m.trackUpload(sid, common.UploadRequest{Path: "/test/expired.txt"}, []string{"chunk1"})
				time.Sleep(2 * time.Millisecond) // Ensure session expires
			},
			setupMocks: func(ms *metadata.MockMetadataStore) {},
			expectErr:  true,
		},
		{
			name:          "error: metadata store failure",
			sessionID:     common.MetadataSessionID("test-session-store-failure"),
			commitTimeout: 5 * time.Second,
			setupManager: func(m *metadataSessionManager, sid common.MetadataSessionID) {
				m.trackUpload(sid, common.UploadRequest{Path: "/test/store_failure.txt"}, []string{"chunk1"})
			},
			setupMocks: func(ms *metadata.MockMetadataStore) {
				ms.On("PutFile", mock.Anything, "/test/store_failure.txt", mock.AnythingOfType("*common.FileInfo")).Return(assert.AnError).Once()
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
