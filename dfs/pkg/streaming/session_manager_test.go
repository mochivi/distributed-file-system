package streaming

import (
	"fmt"
	"log/slog"
	"sync"
	"testing"

	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/internal/config"
	"github.com/mochivi/distributed-file-system/pkg/logging"
	"github.com/stretchr/testify/assert"
)

func newTestSession(sessionID, chunkID string, status SessionStatus) *streamingSession {
	return &streamingSession{
		SessionID: common.StreamingSessionID(sessionID),
		ChunkHeader: common.ChunkHeader{
			ID: chunkID,
		},
		Status: status,
	}
}

func TestNewStreamingSessionManager(t *testing.T) {
	sm := NewStreamingSessionManager(config.DefaultStreamingSessionManagerConfig(), logging.NewTestLogger(slog.LevelError, true))
	assert.NotNil(t, sm)
	assert.NotNil(t, sm.sessions)
	assert.Empty(t, sm.sessions)
}

func TestStreamingSessionManager_Store(t *testing.T) {
	testCases := []struct {
		name           string
		initialState   map[common.StreamingSessionID]*streamingSession
		sessionToStore *streamingSession
		expectErr      bool
	}{
		{
			name:           "success: store new session",
			initialState:   make(map[common.StreamingSessionID]*streamingSession),
			sessionToStore: newTestSession("session1", "chunk1", SessionActive),
			expectErr:      false,
		},
		{
			name: "error: session for chunk already exists",
			initialState: map[common.StreamingSessionID]*streamingSession{
				"existing-session": newTestSession("existing-session", "chunk1", SessionActive),
			},
			sessionToStore: newTestSession("new-session", "chunk1", SessionActive),
			expectErr:      true,
		},
		{
			name: "success: session for chunk exists but not active",
			initialState: map[common.StreamingSessionID]*streamingSession{
				"existing-session": newTestSession("existing-session", "chunk1", SessionCompleted),
			},
			sessionToStore: newTestSession("new-session", "chunk1", SessionActive),
			expectErr:      false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			sm := NewStreamingSessionManager(config.DefaultStreamingSessionManagerConfig(), logging.NewTestLogger(slog.LevelError, true))
			sm.sessions = tc.initialState

			err := sm.Store(tc.sessionToStore.SessionID, tc.sessionToStore)

			if tc.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				storedSession, err := sm.Load(tc.sessionToStore.SessionID)
				assert.NoError(t, err)
				assert.Equal(t, tc.sessionToStore, storedSession)
			}
		})
	}
}

func TestStreamingSessionManager_Load(t *testing.T) {
	session1 := newTestSession("session1", "chunk1", SessionActive)
	testCases := []struct {
		name            string
		initialState    map[common.StreamingSessionID]*streamingSession
		sessionIDToLoad common.StreamingSessionID
		expectedErr     error
		expectedSession *streamingSession
	}{
		{
			name: "success: session found",
			initialState: map[common.StreamingSessionID]*streamingSession{
				session1.SessionID: session1,
			},
			sessionIDToLoad: session1.SessionID,
			expectedErr:     nil,
			expectedSession: session1,
		},
		{
			name:            "failure: session not found",
			initialState:    make(map[common.StreamingSessionID]*streamingSession),
			sessionIDToLoad: common.NewStreamingSessionID(),
			expectedErr:     ErrSessionNotFound,
			expectedSession: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			sm := NewStreamingSessionManager(config.DefaultStreamingSessionManagerConfig(), logging.NewTestLogger(slog.LevelError, true))
			sm.sessions = tc.initialState

			session, err := sm.Load(tc.sessionIDToLoad)

			assert.Equal(t, tc.expectedErr, err)
			assert.Equal(t, tc.expectedSession, session)
		})
	}
}

func TestStreamingSessionManager_Delete(t *testing.T) {
	sm := NewStreamingSessionManager(config.DefaultStreamingSessionManagerConfig(), logging.NewTestLogger(slog.LevelError, true))
	session := newTestSession("session-to-delete", "chunk-to-delete", SessionActive)

	// Store it first
	err := sm.Store(session.SessionID, session)
	assert.NoError(t, err)

	// Make sure it's there
	_, err = sm.Load(session.SessionID)
	assert.NoError(t, err)

	// Delete it
	sm.Delete(session.SessionID)

	// Make sure it's gone
	_, err = sm.Load(session.SessionID)
	assert.Error(t, err)
}

func TestStreamingSessionManager_ExistsForChunk(t *testing.T) {
	testCases := []struct {
		name         string
		initialState map[common.StreamingSessionID]*streamingSession
		chunkID      string
		expectExists bool
	}{
		{
			name: "exists and active",
			initialState: map[common.StreamingSessionID]*streamingSession{
				"session1": newTestSession("session1", "chunk1", SessionActive),
			},
			chunkID:      "chunk1",
			expectExists: true,
		},
		{
			name: "exists but not active",
			initialState: map[common.StreamingSessionID]*streamingSession{
				"session1": newTestSession("session1", "chunk1", SessionCompleted),
			},
			chunkID:      "chunk1",
			expectExists: false,
		},
		{
			name:         "does not exist",
			initialState: make(map[common.StreamingSessionID]*streamingSession),
			chunkID:      "chunk-non-existent",
			expectExists: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			sm := NewStreamingSessionManager(config.DefaultStreamingSessionManagerConfig(), logging.NewTestLogger(slog.LevelError, true))
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
		initialState    map[common.StreamingSessionID]*streamingSession
		chunkID         string
		expectedErr     error
		expectedSession *streamingSession
	}{
		{
			name: "success: found",
			initialState: map[common.StreamingSessionID]*streamingSession{
				"session1": session1,
				"session2": newTestSession("session2", "chunk2", SessionActive),
			},
			chunkID:         "chunk1",
			expectedErr:     nil,
			expectedSession: session1,
		},
		{
			name: "not found",
			initialState: map[common.StreamingSessionID]*streamingSession{
				"session2": newTestSession("session2", "chunk2", SessionActive),
			},
			chunkID:         "chunk-non-existent",
			expectedErr:     ErrSessionNotFound,
			expectedSession: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			sm := NewStreamingSessionManager(config.DefaultStreamingSessionManagerConfig(), logging.NewTestLogger(slog.LevelError, true))
			sm.sessions = tc.initialState

			session, err := sm.LoadByChunk(tc.chunkID)
			assert.Equal(t, tc.expectedErr, err)
			assert.Equal(t, tc.expectedSession, session)
		})
	}
}

func TestStreamingSessionManager_Concurrency(t *testing.T) {
	sm := NewStreamingSessionManager(config.DefaultStreamingSessionManagerConfig(), logging.NewTestLogger(slog.LevelError, true))
	var wg sync.WaitGroup
	numGoroutines := 100

	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(i int) {
			defer wg.Done()

			sessionID := common.NewStreamingSessionID()
			chunkID := fmt.Sprintf("chunk-%d", i%10)

			session := newTestSession(sessionID.String(), chunkID, SessionActive)

			_ = sm.Store(session.SessionID, session)

			loadedSession, err := sm.Load(session.SessionID)
			if err == nil {
				assert.Equal(t, session.SessionID, loadedSession.SessionID)
			}

			// 3. ExistsForChunk
			sm.ExistsForChunk(chunkID)

			// 4. LoadByChunk
			sm.LoadByChunk(chunkID)

			// 5. Delete
			sm.Delete(session.SessionID)
		}(i)
	}

	wg.Wait()
}
