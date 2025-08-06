package coordinator

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/internal/storage/metadata"
	"github.com/mochivi/distributed-file-system/pkg/logging"
)

type MetadataSessionManager interface {
	trackUpload(sessionID string, req common.UploadRequest, chunkIDs []string)
	commit(ctx context.Context, sessionID string, chunkInfos []common.ChunkInfo, metaStore metadata.MetadataStore) error
}

type metadataSessionManager struct {
	sessions      map[string]metadataUploadSession
	mu            sync.Mutex
	commitTimeout time.Duration
	logger        *slog.Logger
}

type metadataUploadSession struct {
	id       string
	exp      time.Time
	fileInfo *common.FileInfo
}

func NewMetadataSessionManager(commitTimeout time.Duration, logger *slog.Logger) *metadataSessionManager {
	metadataLogger := logging.ExtendLogger(logger, slog.String(common.LogComponent, common.ComponentCoordinator))
	manager := &metadataSessionManager{
		sessions:      make(map[string]metadataUploadSession),
		commitTimeout: commitTimeout,
		logger:        metadataLogger,
	}
	return manager
}

func newMetadataUploadSession(sessionID string, exp time.Duration, fileInfo *common.FileInfo) metadataUploadSession {
	return metadataUploadSession{
		id:       sessionID,
		exp:      time.Now().Add(exp),
		fileInfo: fileInfo,
	}
}

func (m *metadataSessionManager) trackUpload(sessionID string, req common.UploadRequest, chunkIDs []string) {
	// Create chunk info array
	chunkInfos := make([]common.ChunkInfo, len(chunkIDs))
	for i := range chunkIDs {
		chunkID := chunkIDs[i]
		chunkInfos[i] = common.ChunkInfo{
			Header: common.ChunkHeader{
				ID:       chunkID,
				Size:     0,  // Will be updated when chunk is stored
				Checksum: "", // Will be updated when chunk is stored
			},
			Replicas: nil, // Will be updated when replicas are created
		}
	}

	fileInfo := &common.FileInfo{
		Path:       req.Path,
		Size:       req.Size,
		ChunkCount: len(chunkIDs),
		Chunks:     chunkInfos,
		CreatedAt:  time.Now(),
		Checksum:   req.Checksum,
	}
	m.logger.Info("Tracking upload session", slog.String(common.LogMetadataSessionID, sessionID),
		slog.String(common.LogFilePath, req.Path), slog.Int(common.LogNumChunks, len(chunkIDs)))

	m.mu.Lock()
	m.sessions[sessionID] = newMetadataUploadSession(sessionID, m.commitTimeout, fileInfo)
	m.mu.Unlock()
}

func (m *metadataSessionManager) commit(ctx context.Context, sessionID string, chunkInfos []common.ChunkInfo, metaStore metadata.MetadataStore) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	session, ok := m.sessions[sessionID]
	if !ok {
		return errors.New("session not found")
	}

	// This is done to avoid scenarios where the metadata update is pending forever
	if time.Now().After(session.exp) {
		return errors.New("session expired")
	}

	// Update the file info with the new chunk infos
	// This is done as we do not know for sure where each chunk is stored by the time the session is created
	fileInfo := session.fileInfo
	fileInfo.Chunks = chunkInfos

	m.logger.Info("Committing metadata for file", slog.String(common.LogFilePath, fileInfo.Path),
		slog.Int(common.LogNumChunks, len(chunkInfos)))
	if err := metaStore.PutFile(ctx, fileInfo.Path, fileInfo); err != nil {
		return fmt.Errorf("failed to store file metadata: %w", err)
	}

	// Clean up the session
	delete(m.sessions, sessionID)
	return nil
}
