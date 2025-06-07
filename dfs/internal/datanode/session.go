package datanode

import (
	"bytes"
	"crypto/sha256"
	"hash"
	"sync"
	"time"

	"github.com/mochivi/distributed-file-system/internal/common"
)

// SessionManager handles currently open chunk streaming sessions with clients
type SessionManager struct {
	sessions map[string]*StreamingSession
	mu       sync.RWMutex
}

// StreamingSession controls the data flow during a chunk streaming session
type StreamingSession struct {
	SessionID    string
	ChunkID      string
	ExpectedSize int
	ExpectedHash string
	CreatedAt    time.Time
	ExpiresAt    time.Time

	// Runtime state
	BytesReceived int64
	Buffer        *bytes.Buffer
	Checksum      hash.Hash // Running checksum calculation

	// Concurrency control
	mutex  sync.RWMutex
	Status SessionStatus
}

func NewSessionManager() *SessionManager {
	return &SessionManager{sessions: make(map[string]*StreamingSession)}
}

func (sm *SessionManager) Store(sessionID string, session *StreamingSession) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.sessions[sessionID] = session
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

// DataNode creates and stores a session
func (s *DataNodeServer) createStreamingSession(sessionId string, req common.ReplicateChunkRequest) {
	session := &StreamingSession{
		SessionID:    sessionId,
		ChunkID:      req.ChunkID,
		ExpectedSize: req.ChunkSize,
		ExpectedHash: req.Checksum,
		CreatedAt:    time.Now(),
		ExpiresAt:    time.Now().Add(s.Config.Session.SessionTimeout),
		Status:       SessionActive,
		Checksum:     sha256.New(),
	}
	s.sessionManager.Store(sessionId, session)
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

	return session
}
