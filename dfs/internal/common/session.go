package common

import (
	"errors"

	"github.com/google/uuid"
)

var ErrInvalidSessionID = errors.New("invalid session id")

type StreamingSessionID string

func NewStreamingSessionID() StreamingSessionID {
	return StreamingSessionID(uuid.NewString())
}
func (s StreamingSessionID) valid() error {
	if s == "" {
		return ErrInvalidSessionID
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
func (s MetadataSessionID) valid() error {
	if s == "" {
		return ErrInvalidSessionID
	}
	return nil
}
func (s MetadataSessionID) String() string {
	return string(s)
}
