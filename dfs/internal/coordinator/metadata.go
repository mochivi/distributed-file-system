package coordinator

import (
	"errors"
	"time"

	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/internal/storage"
)

type metadataUploadSession struct {
	id       string
	exp      time.Time
	fileInfo *common.FileInfo
}

func newMetadataUploadSession(sessionID string, exp time.Duration, fileInfo *common.FileInfo) metadataUploadSession {
	return metadataUploadSession{
		id:       sessionID,
		exp:      time.Now().Add(exp),
		fileInfo: fileInfo,
	}
}

type metadataManager struct {
	sessions      map[string]metadataUploadSession
	commitTimeout time.Duration
}

func newMetadataManager(commitTimeout int) *metadataManager {
	manager := &metadataManager{
		sessions:      make(map[string]metadataUploadSession),
		commitTimeout: time.Duration(commitTimeout),
	}

	go manager.checkExpiry()

	return manager
}

func (m metadataManager) checkExpiry() {
	for {
		ticker := time.NewTicker(5 * time.Second)

		for _, session := range m.sessions {
			if time.Now().After(session.exp) {
				delete(m.sessions, session.id)
			}
		}

		<-ticker.C
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

	m.sessions[sessionID] = newMetadataUploadSession(sessionID, m.commitTimeout, fileInfo)
}

func (m *metadataManager) commit(sessionID string, chunkInfos []common.ChunkInfo, metaStore storage.MetadataStore) error {
	session, ok := m.sessions[sessionID]
	if !ok {
		return errors.New("session not found")
	}

	fileInfo := session.fileInfo
	fileInfo.Chunks = chunkInfos

	metaStore.PutFile(fileInfo.Path, fileInfo)
	return nil
}
