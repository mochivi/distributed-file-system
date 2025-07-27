package streaming

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/stretchr/testify/assert"
)

func TestStreamingSession_startStreamingSession(t *testing.T) {
	testCases := []struct {
		name    string
		session *streamingSession
		chunk   common.ChunkDataStream
	}{
		{
			name: "success: session start",
			session: &streamingSession{
				SessionID: "test-session",
				ChunkHeader: common.ChunkHeader{
					ID:       "test-chunk",
					Index:    0,
					Size:     1024,
					Checksum: "test-checksum",
				},
			},
			chunk: common.ChunkDataStream{
				SessionID: "test-session",
				ChunkID:   "test-chunk",
				Data:      []byte("test data"),
				Offset:    0,
				IsFinal:   false,
			},
		},
		{
			name: "success: session with zero size",
			session: &streamingSession{
				SessionID: "test-session",
				ChunkHeader: common.ChunkHeader{
					ID:       "test-chunk",
					Index:    0,
					Size:     0,
					Checksum: "test-checksum",
				},
			},
			chunk: common.ChunkDataStream{
				SessionID: "test-session",
				ChunkID:   "test-chunk",
				Data:      []byte{},
				Offset:    0,
				IsFinal:   true,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.session.startStreamingSession()

			assert.Equal(t, SessionActive, tc.session.Status) // Session is now active
			assert.NotNil(t, tc.session.buffer)               // Buffer is created when the session is started
			assert.Equal(t, tc.session.offset, 0)             // Nothing written yet
		})
	}
}

func TestStreamingSession_write(t *testing.T) {
	testCases := []struct {
		name        string
		session     *streamingSession
		chunk       common.ChunkDataStream
		expectErr   bool
		expectedErr string
	}{
		{
			name: "success: normal write",
			session: &streamingSession{
				SessionID: "test-session",
				ChunkHeader: common.ChunkHeader{
					ID:       "test-chunk",
					Index:    0,
					Size:     1024,
					Checksum: "test-checksum",
				},
				buffer:          nil, // Will be set by startStreamingSession
				offset:          0,
				runningChecksum: sha256.New(),
			},
			chunk: common.ChunkDataStream{
				SessionID: "test-session",
				ChunkID:   "test-chunk",
				Data:      []byte("test data"),
				Offset:    0,
				IsFinal:   false,
			},
			expectErr: false,
		},
		{
			name: "success: sequential writes",
			session: &streamingSession{
				SessionID: "test-session",
				ChunkHeader: common.ChunkHeader{
					ID:       "test-chunk",
					Index:    0,
					Size:     1024,
					Checksum: "test-checksum",
				},
				buffer:          nil, // Will be set by startStreamingSession
				offset:          0,
				runningChecksum: sha256.New(),
			},
			chunk: common.ChunkDataStream{
				SessionID: "test-session",
				ChunkID:   "test-chunk",
				Data:      []byte("more data"),
				Offset:    0,
				IsFinal:   false,
			},
			expectErr: false,
		},
		{
			name: "error: data out of order",
			session: &streamingSession{
				SessionID: "test-session",
				ChunkHeader: common.ChunkHeader{
					ID:       "test-chunk",
					Index:    0,
					Size:     1024,
					Checksum: "test-checksum",
				},
				buffer:          nil, // Will be set by startStreamingSession
				offset:          10,
				runningChecksum: sha256.New(),
			},
			chunk: common.ChunkDataStream{
				SessionID: "test-session",
				ChunkID:   "test-chunk",
				Data:      []byte("test data"),
				Offset:    5, // Out of order
				IsFinal:   false,
			},
			expectErr:   true,
			expectedErr: "data out of order",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.session.startStreamingSession()
			err := tc.session.write(tc.chunk)

			if tc.expectErr {
				assert.Error(t, err)
				if tc.expectedErr != "" {
					assert.Contains(t, err.Error(), tc.expectedErr)
				}
			} else {
				assert.NoError(t, err)
				// Verify offset was updated
				expectedOffset := tc.chunk.Offset + len(tc.chunk.Data)
				assert.Equal(t, expectedOffset, tc.session.offset)
			}
		})
	}
}

func TestStreamingSession_finalizeSession(t *testing.T) {
	testCases := []struct {
		name        string
		session     *streamingSession
		expectErr   bool
		expectedErr string
	}{
		{
			name: "success: valid checksum",
			session: func() *streamingSession {
				session := &streamingSession{
					SessionID: "test-session",
					ChunkHeader: common.ChunkHeader{
						ID:       "test-chunk",
						Index:    0,
						Size:     1024,
						Checksum: "",
					},
					runningChecksum: sha256.New(),
				}
				// Calculate expected checksum
				data := []byte("test data")
				session.runningChecksum.Write(data)
				expectedChecksum := hex.EncodeToString(session.runningChecksum.Sum(nil))
				session.Checksum = expectedChecksum
				// Reset checksum for the test
				session.runningChecksum = sha256.New()
				session.runningChecksum.Write(data)
				return session
			}(),
			expectErr: false,
		},
		{
			name: "error: checksum mismatch",
			session: func() *streamingSession {
				session := &streamingSession{
					SessionID: "test-session",
					ChunkHeader: common.ChunkHeader{
						ID:       "test-chunk",
						Index:    0,
						Size:     1024,
						Checksum: "invalid-checksum",
					},
					runningChecksum: sha256.New(),
				}
				// Write different data than expected
				session.runningChecksum.Write([]byte("different data"))
				return session
			}(),
			expectErr:   true,
			expectedErr: "checksum mismatch",
		},
		{
			name: "success: empty data with valid checksum",
			session: func() *streamingSession {
				session := &streamingSession{
					SessionID: "test-session",
					ChunkHeader: common.ChunkHeader{
						ID:       "test-chunk",
						Index:    0,
						Size:     0,
						Checksum: "",
					},
					runningChecksum: sha256.New(),
				}
				// Calculate expected checksum for empty data
				expectedChecksum := hex.EncodeToString(session.runningChecksum.Sum(nil))
				session.Checksum = expectedChecksum
				return session
			}(),
			expectErr: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.session.finalizeSession()

			if tc.expectErr {
				assert.Error(t, err)
				if tc.expectedErr != "" {
					assert.Contains(t, err.Error(), tc.expectedErr)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestStreamingSession_Fail(t *testing.T) {
	testCases := []struct {
		name           string
		session        *streamingSession
		expectPanic    bool
		expectedStatus SessionStatus
	}{
		{
			name: "success: fail active session",
			session: &streamingSession{
				SessionID: "test-session",
				Status:    SessionActive,
			},
			expectPanic:    false,
			expectedStatus: SessionFailed,
		},
		{
			name: "panic: fail completed session",
			session: &streamingSession{
				SessionID: "test-session",
				Status:    SessionCompleted,
			},
			expectPanic:    true,
			expectedStatus: SessionCompleted,
		},
		{
			name: "panic: fail already failed session",
			session: &streamingSession{
				SessionID: "test-session",
				Status:    SessionFailed,
			},
			expectPanic:    true,
			expectedStatus: SessionFailed,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.expectPanic {
				assert.Panics(t, func() {
					tc.session.Fail()
				})
			} else {
				assert.NotPanics(t, func() {
					tc.session.Fail()
				})
				assert.Equal(t, tc.expectedStatus, tc.session.Status)
			}
		})
	}
}

func TestStreamingSession_Complete(t *testing.T) {
	testCases := []struct {
		name           string
		session        *streamingSession
		expectPanic    bool
		expectedStatus SessionStatus
	}{
		{
			name: "success: complete active session",
			session: &streamingSession{
				SessionID: "test-session",
				Status:    SessionActive,
			},
			expectPanic:    false,
			expectedStatus: SessionCompleted,
		},
		{
			name: "panic: complete already completed session",
			session: &streamingSession{
				SessionID: "test-session",
				Status:    SessionCompleted,
			},
			expectPanic:    true,
			expectedStatus: SessionCompleted,
		},
		{
			name: "panic: complete failed session",
			session: &streamingSession{
				SessionID: "test-session",
				Status:    SessionFailed,
			},
			expectPanic:    true,
			expectedStatus: SessionFailed,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.expectPanic {
				assert.Panics(t, func() {
					tc.session.Complete()
				})
			} else {
				assert.NotPanics(t, func() {
					tc.session.Complete()
				})
				assert.Equal(t, tc.expectedStatus, tc.session.Status)
			}
		})
	}
}

func TestStreamingSession_Integration(t *testing.T) {
	testCases := []struct {
		name        string
		chunks      []common.ChunkDataStream
		expectErr   bool
		expectedErr string
	}{
		{
			name: "success: complete streaming session",
			chunks: []common.ChunkDataStream{
				{
					SessionID: "test-session",
					ChunkID:   "test-chunk",
					Data:      []byte("hello"),
					Offset:    0,
					IsFinal:   false,
				},
				{
					SessionID: "test-session",
					ChunkID:   "test-chunk",
					Data:      []byte(" world"),
					Offset:    5,
					IsFinal:   true,
				},
			},
			expectErr: false,
		},
		{
			name: "error: out of order chunks",
			chunks: []common.ChunkDataStream{
				{
					SessionID: "test-session",
					ChunkID:   "test-chunk",
					Data:      []byte("hello"),
					Offset:    0,
					IsFinal:   false,
				},
				{
					SessionID: "test-session",
					ChunkID:   "test-chunk",
					Data:      []byte(" world"),
					Offset:    3, // Should be 5
					IsFinal:   true,
				},
			},
			expectErr:   true,
			expectedErr: "data out of order",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create session
			session := &streamingSession{
				SessionID: "test-session",
				ChunkHeader: common.ChunkHeader{
					ID:       "test-chunk",
					Index:    0,
					Size:     1024,
					Checksum: "",
				},
				runningChecksum: sha256.New(),
			}

			// Calculate expected checksum from all chunks
			expectedChecksum := sha256.New()
			for _, chunk := range tc.chunks {
				expectedChecksum.Write(chunk.Data)
			}
			session.Checksum = hex.EncodeToString(expectedChecksum.Sum(nil))

			// Start session with first chunk
			session.startStreamingSession()

			// Process chunks
			for i := 0; i < len(tc.chunks); i++ {
				err := session.write(tc.chunks[i])
				if err != nil {
					if tc.expectErr {
						assert.Contains(t, err.Error(), tc.expectedErr)
						return
					}
					t.Fatalf("Failed to write chunk %d: %v", i, err)
				}
			}

			// Finalize session if no errors
			if !tc.expectErr {
				err := session.finalizeSession()
				assert.NoError(t, err)
				assert.Equal(t, SessionActive, session.Status)
			}
		})
	}
}
