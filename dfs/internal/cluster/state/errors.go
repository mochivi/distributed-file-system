package state

import "errors"

// Store level errors
var (
	ErrNotFound         = errors.New("node not found")
	ErrNoAvailableNodes = errors.New("no available nodes")
	ErrVersionTooOld    = errors.New("version too old")
)
