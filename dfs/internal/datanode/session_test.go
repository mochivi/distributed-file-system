package datanode

import (
	"fmt"
	"log/slog"
	"sync"
	"testing"

	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/pkg/logging"
	"github.com/stretchr/testify/assert"
)

func newTestSession(sessionID, chunkID string, status SessionStatus) *StreamingSession {
	return &StreamingSession{
		SessionID: sessionID,
		ChunkHeader: common.ChunkHeader{
			ID: chunkID,
		},
		Status: status,
		logger: logging.NewTestLogger(slog.LevelDebug),
	}
}

func TestNewStreamingSessionManager(t *testing.T) {
	sm := NewStreamingSessionManager()
	assert.NotNil(t, sm)
	assert.NotNil(t, sm.sessions)
	assert.Empty(t, sm.sessions)
}

func TestStreamingSessionManager_Store(t *testing.T) {
	testCases := []struct {
		name           string
		initialState   map[string]*StreamingSession
		sessionToStore *StreamingSession
		expectErr      bool
	}{
		{
			name:           "success: store new session",
			initialState:   make(map[string]*StreamingSession),
			sessionToStore: newTestSession("session1", "chunk1", SessionActive),
			expectErr:      false,
		},
		{
			name: "error: session for chunk already exists",
			initialState: map[string]*StreamingSession{
				"existing-session": newTestSession("existing-session", "chunk1", SessionActive),
			},
			sessionToStore: newTestSession("new-session", "chunk1", SessionActive),
			expectErr:      true,
		},
		{
			name: "success: session for chunk exists but not active",
			initialState: map[string]*StreamingSession{
				"existing-session": newTestSession("existing-session", "chunk1", SessionCompleted),
			},
			sessionToStore: newTestSession("new-session", "chunk1", SessionActive),
			expectErr:      false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			sm := NewStreamingSessionManager()
			sm.sessions = tc.initialState

			err := sm.Store(tc.sessionToStore.SessionID, tc.sessionToStore)

			if tc.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				storedSession, ok := sm.Load(tc.sessionToStore.SessionID)
				assert.True(t, ok)
				assert.Equal(t, tc.sessionToStore, storedSession)
			}
		})
	}
}

func TestStreamingSessionManager_Load(t *testing.T) {
	session1 := newTestSession("session1", "chunk1", SessionActive)
	testCases := []struct {
		name            string
		initialState    map[string]*StreamingSession
		sessionIDToLoad string
		expectFound     bool
		expectedSession *StreamingSession
	}{
		{
			name: "success: session found",
			initialState: map[string]*StreamingSession{
				"session1": session1,
			},
			sessionIDToLoad: "session1",
			expectFound:     true,
			expectedSession: session1,
		},
		{
			name:            "failure: session not found",
			initialState:    make(map[string]*StreamingSession),
			sessionIDToLoad: "non-existent",
			expectFound:     false,
			expectedSession: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			sm := NewStreamingSessionManager()
			sm.sessions = tc.initialState

			session, found := sm.Load(tc.sessionIDToLoad)

			assert.Equal(t, tc.expectFound, found)
			assert.Equal(t, tc.expectedSession, session)
		})
	}
}

func TestStreamingSessionManager_Delete(t *testing.T) {
	sm := NewStreamingSessionManager()
	session := newTestSession("session-to-delete", "chunk-to-delete", SessionActive)

	// Store it first
	err := sm.Store(session.SessionID, session)
	assert.NoError(t, err)

	// Make sure it's there
	_, ok := sm.Load(session.SessionID)
	assert.True(t, ok)

	// Delete it
	sm.Delete(session.SessionID)

	// Make sure it's gone
	_, ok = sm.Load(session.SessionID)
	assert.False(t, ok)
}

func TestStreamingSessionManager_ExistsForChunk(t *testing.T) {
	testCases := []struct {
		name         string
		initialState map[string]*StreamingSession
		chunkID      string
		expectExists bool
	}{
		{
			name: "exists and active",
			initialState: map[string]*StreamingSession{
				"session1": newTestSession("session1", "chunk1", SessionActive),
			},
			chunkID:      "chunk1",
			expectExists: true,
		},
		{
			name: "exists but not active",
			initialState: map[string]*StreamingSession{
				"session1": newTestSession("session1", "chunk1", SessionCompleted),
			},
			chunkID:      "chunk1",
			expectExists: false,
		},
		{
			name:         "does not exist",
			initialState: make(map[string]*StreamingSession),
			chunkID:      "chunk-non-existent",
			expectExists: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			sm := NewStreamingSessionManager()
			sm.sessions = tc.initialState

			exists := sm.ExistsForChunk(tc.chunkID)
			assert.Equal(t, tc.expectExists, exists)
		})
	}
}

func TestStreamingSessionManager_LoadByChunk(t *testing.T) {
	session1 := newTestSession("session1", "chunk1", SessionActive)

	testCases := []struct {
		name            string
		initialState    map[string]*StreamingSession
		chunkID         string
		expectFound     bool
		expectedSession *StreamingSession
	}{
		{
			name: "success: found",
			initialState: map[string]*StreamingSession{
				"session1": session1,
				"session2": newTestSession("session2", "chunk2", SessionActive),
			},
			chunkID:         "chunk1",
			expectFound:     true,
			expectedSession: session1,
		},
		{
			name: "not found",
			initialState: map[string]*StreamingSession{
				"session2": newTestSession("session2", "chunk2", SessionActive),
			},
			chunkID:         "chunk-non-existent",
			expectFound:     false,
			expectedSession: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			sm := NewStreamingSessionManager()
			sm.sessions = tc.initialState

			session, found := sm.LoadByChunk(tc.chunkID)
			assert.Equal(t, tc.expectFound, found)
			assert.Equal(t, tc.expectedSession, session)
		})
	}
}

func TestStreamingSessionManager_Concurrency(t *testing.T) {
	sm := NewStreamingSessionManager()
	var wg sync.WaitGroup
	numGoroutines := 100

	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(i int) {
			defer wg.Done()

			sessionID := fmt.Sprintf("session-%d", i)
			chunkID := fmt.Sprintf("chunk-%d", i%10)

			session := newTestSession(sessionID, chunkID, SessionActive)

			_ = sm.Store(sessionID, session)

			loadedSession, ok := sm.Load(sessionID)
			if ok {
				assert.Equal(t, sessionID, loadedSession.SessionID)
			}

			// 3. ExistsForChunk
			sm.ExistsForChunk(chunkID)

			// 4. LoadByChunk
			sm.LoadByChunk(chunkID)

			// 5. Delete
			sm.Delete(sessionID)
		}(i)
	}

	wg.Wait()
}
