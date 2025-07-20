package coordinator

import (
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/internal/storage"
	"github.com/mochivi/distributed-file-system/internal/storage/chunk"
	"github.com/mochivi/distributed-file-system/pkg/logging"
)

type MetadataSessionManager interface {
	trackUpload(sessionID string, req common.UploadRequest, numChunks int)
	commit(sessionID string, chunkInfos []common.ChunkInfo, metaStore storage.MetadataStore) error
}

type metadataSessionManager struct {
	sessions      map[string]metadataUploadSession
	commitTimeout time.Duration
	logger        *slog.Logger
}

type metadataUploadSession struct {
	id       string
	exp      time.Time
	fileInfo *common.FileInfo
}

func NewMetadataSessionManager(commitTimeout time.Duration, logger *slog.Logger) *metadataSessionManager {
	metadataLogger := logging.ExtendLogger(logger, slog.String("component", "metadata_manager"))
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

func (m *metadataSessionManager) trackUpload(sessionID string, req common.UploadRequest, numChunks int) {
	// Create chunk info array
	chunkInfos := make([]common.ChunkInfo, numChunks)
	for i := range numChunks {
		chunkID := chunk.FormatChunkID(req.Path, i)
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
		ChunkCount: numChunks,
		Chunks:     chunkInfos,
		CreatedAt:  time.Now(),
		Checksum:   req.Checksum,
	}
	m.logger.Info("Tracking upload session", slog.String("session_id", sessionID), slog.String("file_path", req.Path), slog.Int("num_chunks", numChunks))
	m.sessions[sessionID] = newMetadataUploadSession(sessionID, m.commitTimeout, fileInfo)
}

func (m *metadataSessionManager) commit(sessionID string, chunkInfos []common.ChunkInfo, metaStore storage.MetadataStore) error {
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

	m.logger.Info("Committing metadata for file", slog.String("file_path", fileInfo.Path), slog.Int("num_chunks", len(chunkInfos)))
	if err := metaStore.PutFile(fileInfo.Path, fileInfo); err != nil {
		return fmt.Errorf("failed to store file metadata: %w", err)
	}

	// Clean up the session
	delete(m.sessions, sessionID)
	return nil
}
