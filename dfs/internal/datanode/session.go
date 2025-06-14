package datanode

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"hash"
	"log/slog"
	"sync"
	"time"

	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/pkg/logging"
)

// SessionManager handles currently open chunk streaming sessions with clients
type SessionManager struct {
	sessions map[string]*StreamingSession
	mu       sync.RWMutex
}

// StreamingSession controls the data flow during a chunk streaming session
type StreamingSession struct {
	SessionID string
	CreatedAt time.Time
	ExpiresAt time.Time

	// Chunk metadata
	common.ChunkMeta

	// Runtime state
	BytesReceived   int64
	Buffer          *bytes.Buffer
	RunningChecksum hash.Hash // Running checksum calculation

	// Concurrency control
	mutex  sync.RWMutex
	Status SessionStatus

	// scoped logger for this session
	logger *slog.Logger
}

func NewSessionManager() *SessionManager {
	return &SessionManager{sessions: make(map[string]*StreamingSession)}
}

func (sm *SessionManager) Store(sessionID string, session *StreamingSession) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if sm.ExistsForChunk(session.ChunkID) {
		return fmt.Errorf("session for chunk %s already exists", session.ChunkID)
	}

	sm.sessions[sessionID] = session
	return nil
}

func (sm *SessionManager) Load(sessionID string) (*StreamingSession, bool) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	session, ok := sm.sessions[sessionID]
	if !ok {
		return nil, false
	}
	return session, true
}

func (sm *SessionManager) Delete(sessionID string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	delete(sm.sessions, sessionID)
}

// Temporary solution to check if a chunk is already being streamed, stops duplicate requests
func (sm *SessionManager) ExistsForChunk(chunkID string) bool {
	for _, session := range sm.sessions {
		if session.ChunkID == chunkID {
			return true
		}
	}
	return false
}

// DataNode creates and stores a session
func (s *DataNodeServer) createStreamingSession(sessionId string, chunkMeta common.ChunkMeta, logger *slog.Logger) error {
	streamLogger := logging.OperationLogger(logger, "create_streaming_session", slog.String("session_id", sessionId))
	session := &StreamingSession{
		SessionID:       sessionId,
		ChunkMeta:       chunkMeta,
		CreatedAt:       time.Now(),
		ExpiresAt:       time.Now().Add(s.Config.Session.SessionTimeout),
		Status:          SessionActive,
		RunningChecksum: sha256.New(),
		logger:          streamLogger,
	}
	if err := s.sessionManager.Store(sessionId, session); err != nil {
		return err
	}
	streamLogger.Info("Streaming session created")
	return nil
}

// DataNode retrieves the streaming session
func (s *DataNodeServer) getStreamingSession(sessionId string) *StreamingSession {
	session, exists := s.sessionManager.Load(sessionId)
	if !exists {
		return nil
	}

	// Check if expired
	if time.Now().After(session.ExpiresAt) {
		s.sessionManager.Delete(sessionId)
		return nil
	}

	session.logger.Debug("Streaming session retrieved")

	return session
}
