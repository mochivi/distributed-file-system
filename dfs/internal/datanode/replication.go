package datanode

type ReplicationManager struct {
}

func (rm *ReplicationManager) replicate(chunkID string, data []byte, requiredReplicas int) error {
	return nil
}
