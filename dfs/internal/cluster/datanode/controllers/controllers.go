package datanode_controllers

type NodeAgentControllers struct {
	Heartbeat        HeartbeatProvider
	GarbageCollector OrphanedChunksGCProvider
}

func NewNodeAgentControllers(heartbeatController HeartbeatProvider, garbageCollector OrphanedChunksGCProvider) NodeAgentControllers {
	return NodeAgentControllers{
		Heartbeat:        heartbeatController,
		GarbageCollector: garbageCollector,
	}
}
