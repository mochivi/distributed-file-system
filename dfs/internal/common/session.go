package common

import (
	"errors"
	"fmt"

	"github.com/google/uuid"
)

var ErrInvalidSessionID = errors.New("invalid session id")

type StreamingSessionID string

func NewStreamingSessionID() StreamingSessionID {
	return StreamingSessionID(uuid.NewString())
}
func (s StreamingSessionID) validate() error {
	if s == "" {
		return fmt.Errorf("%w: session id is required", ErrInvalidSessionID)
	}
	return nil
}
func (s StreamingSessionID) String() string {
	return string(s)
}

type MetadataSessionID string

func NewMetadataSessionID() MetadataSessionID {
	return MetadataSessionID(uuid.NewString())
}
func (s MetadataSessionID) validate() error {
	if s == "" {
		return fmt.Errorf("%w: session id is required", ErrInvalidSessionID)
	}
	return nil
}
func (s MetadataSessionID) String() string {
	return string(s)
}
