package streaming

import (
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/internal/config"
)

// streamingSessionManager handles currently open chunk streaming sessions with clients
type streamingSessionManager struct {
	sessions map[string]*streamingSession
	mu       sync.RWMutex
	config   config.StreamingSessionManagerConfig
	logger   *slog.Logger
}

func NewStreamingSessionManager(config config.StreamingSessionManagerConfig, logger *slog.Logger) *streamingSessionManager {
	return &streamingSessionManager{sessions: make(map[string]*streamingSession), config: config, logger: logger}
}

func (sm *streamingSessionManager) NewSession(chunkHeader common.ChunkHeader, propagate bool) *streamingSession {
	sessionID := uuid.New().String()
	session := NewStreamingSession(sessionID, chunkHeader, propagate)
	session.ExpiresAt = time.Now().Add(sm.config.SessionTimeout)
	return session
}

func (sm *streamingSessionManager) GetSession(sessionID string) (*streamingSession, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	session, ok := sm.sessions[sessionID]
	if !ok {
		return nil, false
	}

	// Check if expired
	if time.Now().After(session.ExpiresAt) {
		sm.Delete(sessionID)
		return nil, false
	}

	return session, true
}

func (sm *streamingSessionManager) Store(sessionID string, session *streamingSession) error {
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

func (sm *streamingSessionManager) Load(sessionID string) (*streamingSession, bool) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	session, ok := sm.sessions[sessionID]
	if !ok {
		sm.logger.Error("Session not found", slog.String("session_id", sessionID), slog.Any("available_sessions", sm.sessions))
		return nil, false
	}
	return session, true
}

func (sm *streamingSessionManager) Delete(sessionID string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	delete(sm.sessions, sessionID)
}

// Temporary solution to check if a chunk is already being streamed, stops duplicate requests
func (sm *streamingSessionManager) ExistsForChunk(chunkID string) bool {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	for _, session := range sm.sessions {
		if session.ChunkHeader.ID == chunkID && session.Status == SessionActive {
			return true
		}
	}
	return false
}

func (sm *streamingSessionManager) LoadByChunk(chunkID string) (*streamingSession, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	for _, session := range sm.sessions {
		if session.ChunkHeader.ID == chunkID {
			return session, true
		}
	}
	return nil, false
}
