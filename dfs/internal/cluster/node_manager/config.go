package node_manager

type NodeManagerConfig struct {
	CoordinatorNodeManagerConfig CoordinatorNodeManagerConfig
	DataNodeManagerConfig        DataNodeManagerConfig
}

type CoordinatorNodeManagerConfig struct {
}

type DataNodeManagerConfig struct {
	MaxHistorySize int // max number of updates to keep in history
}

func DefaultDataNodeManagerConfig() DataNodeManagerConfig {
	return DataNodeManagerConfig{
		MaxHistorySize: 1000,
	}
}

func DefaultNodeManagerConfig() NodeManagerConfig {
	return NodeManagerConfig{
		CoordinatorNodeManagerConfig: CoordinatorNodeManagerConfig{},
		DataNodeManagerConfig:        DefaultDataNodeManagerConfig(),
	}
}
