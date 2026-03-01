package datanode

import "github.com/mochivi/distributed-file-system/internal/common"

// System resources management

func (s *service) hasCapacity(chunkSize common.FileSize) bool {
	return true
}
