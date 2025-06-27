package cluster

import "fmt"

func (c *ClusterNode) Run() error {
	// TODO: implement service discovery -- right now, reading from environment variables
	if err := c.nodeManager.BootstrapCoordinatorNode(); err != nil {
		return fmt.Errorf("failed to bootstrap coordinator node: %w", err)
	}

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.controllers.heartbeat.Run(c.config.Node, c.nodeManager)
	}()

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.services.register.RegisterWithCoordinator(c.ctx, c.config.Node, c.nodeManager)
	}()

	c.wg.Wait()

	return nil
}
