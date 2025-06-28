package cluster

import "fmt"

func (c *NodeAgent) Run() error {
	// TODO: implement service discovery -- right now, reading from environment variables
	if err := c.services.coordinator.BootstrapCoordinatorNode(); err != nil {
		return fmt.Errorf("failed to bootstrap coordinator node: %w", err)
	}

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.controllers.heartbeat.Run(c.info, c.clusterStateManager, c.services.coordinator)
	}()

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.services.register.RegisterWithCoordinator(c.ctx, c.info, c.clusterStateManager, c.services.coordinator)
	}()

	c.wg.Wait()

	return nil
}
