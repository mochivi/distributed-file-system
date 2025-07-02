package datanode

import (
	"github.com/stretchr/testify/mock"
)

type MockStreamingSessionManager struct {
	mock.Mock
}

func (m *MockStreamingSessionManager) Store(sessionID string, session *StreamingSession) error {
	args := m.Called(sessionID, session)
	return args.Error(0)
}

func (m *MockStreamingSessionManager) Load(sessionID string) (*StreamingSession, bool) {
	args := m.Called(sessionID)
	if args.Get(0) == nil {
		return nil, false
	}
	return args.Get(0).(*StreamingSession), args.Bool(1)
}

func (m *MockStreamingSessionManager) Delete(sessionID string) {
	m.Called(sessionID)
}

func (m *MockStreamingSessionManager) ExistsForChunk(chunkID string) bool {
	args := m.Called(chunkID)
	return args.Bool(0)
}

func (m *MockStreamingSessionManager) LoadByChunk(chunkID string) (*StreamingSession, bool) {
	args := m.Called(chunkID)
	if args.Get(0) == nil {
		return nil, false
	}
	return args.Get(0).(*StreamingSession), args.Bool(1)
}
