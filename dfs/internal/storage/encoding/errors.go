package encoding

import "errors"

var (
	ErrSerializeHeader   = errors.New("failed to serialize chunk header")
	ErrDeserializeHeader = errors.New("failed to deserialize chunk header")
	ErrInValidHeader     = errors.New("invalid header")
)
