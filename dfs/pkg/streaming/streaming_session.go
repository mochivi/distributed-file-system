package streaming

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/mochivi/distributed-file-system/internal/common"
)

func (s *streamingSession) startStreamingSession() {
	buf := make([]byte, 0, s.Size) // through chunkHeader
	s.buffer = bytes.NewBuffer(buf)
	s.Status = SessionActive
}

func (s *streamingSession) write(chunk common.ChunkDataStream) error {
	if s.buffer == nil {
		panic("nil buffer for streaming session")
	}

	if chunk.Offset != s.offset {
		return fmt.Errorf("data out of order")
	}

	s.runningChecksum.Write(chunk.Data)
	s.buffer.Write(chunk.Data)
	s.offset += len(chunk.Data)

	return nil
}

func (s *streamingSession) finalizeSession() error {
	computedHash := s.runningChecksum.Sum(nil)
	expectedHash, _ := hex.DecodeString(s.Checksum)

	if !bytes.Equal(computedHash, expectedHash) {
		return errors.New("checksum mismatch")
	}

	return nil
}

func (s *streamingSession) Context() context.Context {
	return s.ctx
}

func (s *streamingSession) Fail() {
	if s.Status != SessionActive {
		// logic says this should never happen with good code
		// we are better off capturing this scenario in integration tests
		panic("trying to update to same status")
	}
	s.Status = SessionFailed
}

func (s *streamingSession) Complete() {
	if s.Status != SessionActive {
		// logic says this should never happen with good code
		// we are better off capturing this scenario in integration tests
		panic("trying to update to same status")
	}
	s.Status = SessionCompleted
}
