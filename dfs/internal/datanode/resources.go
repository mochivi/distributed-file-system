package datanode

// System resources management

func (s *DataNodeServer) hasCapacity(chunkSize int64) bool {
	return true
}
