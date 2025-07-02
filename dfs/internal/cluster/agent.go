package cluster

import "fmt"

func (c *NodeAgent) Run() error {
	// TODO: implement service discovery -- right now, reading from environment variables
	if err := c.services.Coordinator.BootstrapCoordinator(); err != nil {
		return fmt.Errorf("failed to bootstrap coordinator node: %w", err)
	}

	// Register with coordinator before starting the controller loops
	c.services.Register.RegisterWithCoordinator(c.ctx, c.info, c.clusterStateManager, c.services.Coordinator)

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.controllers.Heartbeat.Run(c.info, c.clusterStateManager, c.services.Coordinator)
	}()

	c.wg.Wait()

	return nil
}
