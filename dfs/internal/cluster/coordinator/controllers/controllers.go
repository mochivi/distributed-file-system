package coordinator_controllers

type CoordinatorNodeAgentControllers struct {
	GarbageCollector *DeletedFilesGCController
}

func NewCoordinatorNodeAgentControllers(gc *DeletedFilesGCController) *CoordinatorNodeAgentControllers {
	return &CoordinatorNodeAgentControllers{
		GarbageCollector: gc,
	}
}
