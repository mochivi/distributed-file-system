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
		c.controllers.heartbeat.Run(c.config.node, c.nodeManager)
	}()

	// c.wg.Add(1)
	// go func() {
	// 	defer c.wg.Done()
	// 	c.controllers.gc.Run()
	// }()

	// c.wg.Add(1)
	// go func() {
	// 	defer c.wg.Done()
	// 	c.controllers.register.Run()
	// }()

	c.wg.Wait()

	return nil
}
