package datanode

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"hash"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/internal/config"
	"github.com/mochivi/distributed-file-system/pkg/logging"
)

type SessionManager interface {
	NewSession(chunkHeader common.ChunkHeader, propagate bool) *StreamingSession
	GetSession(sessionID string) (*StreamingSession, bool)
	Store(sessionID string, session *StreamingSession) error
	Load(sessionID string) (*StreamingSession, bool)
	Delete(sessionID string)
	ExistsForChunk(chunkID string) bool
	LoadByChunk(chunkID string) (*StreamingSession, bool)
}

type SessionStatus int

const (
	SessionActive SessionStatus = iota
	SessionCompleted
	SessionFailed
	SessionExpired
)

// StreamingSessionManager handles currently open chunk streaming sessions with clients
type StreamingSessionManager struct {
	sessions map[string]*StreamingSession
	mu       sync.RWMutex
	config   config.StreamingSessionManagerConfig
	logger   *slog.Logger
}

// StreamingSession controls the data flow during a chunk streaming session
type StreamingSession struct {
	SessionID string
	CreatedAt time.Time
	ExpiresAt time.Time

	// Chunk metadata
	common.ChunkHeader
	Propagate bool

	// Runtime state
	BytesReceived   int64
	Buffer          *bytes.Buffer
	RunningChecksum hash.Hash // Running checksum calculation

	// Concurrency control
	// mu     sync.RWMutex
	Status SessionStatus

	// scoped logger for this session
	logger *slog.Logger
}

func NewStreamingSessionManager(config config.StreamingSessionManagerConfig, logger *slog.Logger) *StreamingSessionManager {
	return &StreamingSessionManager{sessions: make(map[string]*StreamingSession), config: config, logger: logger}
}

func (sm *StreamingSessionManager) NewSession(chunkHeader common.ChunkHeader, propagate bool) *StreamingSession {
	sessionID := uuid.New().String()
	streamLogger := logging.OperationLogger(sm.logger, "chunk_streaming_session", slog.String("session_id", sessionID))
	session := &StreamingSession{
		SessionID:       sessionID,
		ChunkHeader:     chunkHeader,
		CreatedAt:       time.Now(),
		ExpiresAt:       time.Now().Add(sm.config.SessionTimeout), // How much the client has until they submit a request initiating the streaming session
		Propagate:       propagate,
		Status:          SessionActive,
		RunningChecksum: sha256.New(),
		logger:          streamLogger,
	}
	return session
}

func (sm *StreamingSessionManager) GetSession(sessionID string) (*StreamingSession, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	session, ok := sm.sessions[sessionID]
	if !ok {
		return nil, false
	}

	return session, true
}

func (sm *StreamingSessionManager) Store(sessionID string, session *StreamingSession) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	for _, s := range sm.sessions {
		if s.ChunkHeader.ID == session.ChunkHeader.ID && s.Status == SessionActive {
			return fmt.Errorf("session for chunk %s already exists", session.ChunkHeader.ID)
		}
	}

	sm.sessions[sessionID] = session
	return nil
}

func (sm *StreamingSessionManager) Load(sessionID string) (*StreamingSession, bool) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	session, ok := sm.sessions[sessionID]
	if !ok {
		return nil, false
	}
	return session, true
}

func (sm *StreamingSessionManager) Delete(sessionID string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	delete(sm.sessions, sessionID)
}

// Temporary solution to check if a chunk is already being streamed, stops duplicate requests
func (sm *StreamingSessionManager) ExistsForChunk(chunkID string) bool {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	for _, session := range sm.sessions {
		if session.ChunkHeader.ID == chunkID && session.Status == SessionActive {
			return true
		}
	}
	return false
}

func (sm *StreamingSessionManager) LoadByChunk(chunkID string) (*StreamingSession, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	for _, session := range sm.sessions {
		if session.ChunkHeader.ID == chunkID {
			return session, true
		}
	}
	return nil, false
}

// DataNode retrieves the streaming session
func (s *DataNodeServer) getStreamingSession(sessionId string) (*StreamingSession, bool) {
	session, exists := s.sessionManager.Load(sessionId)
	if !exists {
		return nil, false
	}

	// Check if expired
	if time.Now().After(session.ExpiresAt) {
		s.sessionManager.Delete(sessionId)
		return nil, false
	}

	session.logger.Debug("Streaming session retrieved")

	return session, true
}
