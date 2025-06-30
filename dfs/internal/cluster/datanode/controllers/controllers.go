package datanode_controllers

type NodeAgentControllers struct {
	Heartbeat HeartbeatProvider
}

func NewNodeAgentControllers(heartbeatController HeartbeatProvider) NodeAgentControllers {
	return NodeAgentControllers{
		Heartbeat: heartbeatController,
	}
}
