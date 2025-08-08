package streaming

import (
	"context"
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

func (sm *streamingSessionManager) NewSession(ctx context.Context, chunkHeader common.ChunkHeader, propagate bool) *streamingSession {
	sessionID := uuid.New().String()
	session := NewStreamingSession(ctx, sessionID, chunkHeader, propagate)
	session.ExpiresAt = time.Now().Add(sm.config.SessionTimeout)
	return session
}

func (sm *streamingSessionManager) GetSession(sessionID string) (*streamingSession, error) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	session, ok := sm.sessions[sessionID]
	if !ok {
		return nil, ErrSessionNotFound
	}

	// Check if expired
	if time.Now().After(session.ExpiresAt) {
		sm.Delete(sessionID)
		return nil, ErrSessionExpired
	}

	return session, nil
}

func (sm *streamingSessionManager) Store(sessionID string, session *streamingSession) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	for _, s := range sm.sessions {
		if s.ChunkHeader.ID == session.ChunkHeader.ID && s.Status == SessionActive {
			return ErrSessionAlreadyExists
		}
	}

	sm.sessions[sessionID] = session
	return nil
}

func (sm *streamingSessionManager) Load(sessionID string) (*streamingSession, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	session, ok := sm.sessions[sessionID]
	if !ok {
		return nil, ErrSessionNotFound
	}
	return session, nil
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

func (sm *streamingSessionManager) LoadByChunk(chunkID string) (*streamingSession, error) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	for _, session := range sm.sessions {
		if session.ChunkHeader.ID == chunkID {
			return session, nil
		}
	}
	return nil, ErrSessionNotFound
}
