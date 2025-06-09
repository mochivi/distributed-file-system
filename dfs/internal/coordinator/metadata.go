package coordinator

import (
	"errors"
	"log"
	"time"

	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/internal/storage"
)

type metadataManager struct {
	sessions      map[string]metadataUploadSession
	commitTimeout time.Duration
}

type metadataUploadSession struct {
	id       string
	exp      time.Time
	fileInfo *common.FileInfo
}

func NewMetadataManager(commitTimeout time.Duration) *metadataManager {
	manager := &metadataManager{
		sessions:      make(map[string]metadataUploadSession),
		commitTimeout: commitTimeout,
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

func (m *metadataManager) trackUpload(sessionID string, req UploadRequest, numChunks int) {
	fileInfo := &common.FileInfo{
		Path:       req.Path,
		Size:       req.Size,
		ChunkCount: numChunks,
		Chunks:     nil,
		CreatedAt:  time.Now(),
		Checksum:   req.Checksum,
	}
	log.Printf("Tracking upload session: %s", sessionID)
	m.sessions[sessionID] = newMetadataUploadSession(sessionID, m.commitTimeout, fileInfo)
}

func (m *metadataManager) commit(sessionID string, chunkInfos []common.ChunkInfo, metaStore storage.MetadataStore) error {
	session, ok := m.sessions[sessionID]
	if !ok {
		return errors.New("session not found")
	}

	// This is done to avoid scenarios where the metadata update is pending forever
	if time.Now().After(session.exp) {
		return errors.New("session expired")
	}

	fileInfo := session.fileInfo
	fileInfo.Chunks = chunkInfos

	metaStore.PutFile(fileInfo.Path, fileInfo)
	return nil
}
