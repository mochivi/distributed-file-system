package cluster

import (
	"fmt"
	"log/slog"
)

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
		if err := c.controllers.Heartbeat.Run(c.info, c.clusterStateManager, c.services.Coordinator); err != nil {
			// TODO: decide what kind of action to take when heartbeat loop dies
			c.logger.Error("Heartbeat loop failed", slog.Any("error", err))
		}
	}()

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		if err := c.controllers.GarbageCollector.Run(); err != nil {
			// TODO: decide what kind of action to take when gc loop dies, likely just restart it
			c.logger.Error("OrphanedChunk garbage collection loop failed", slog.Any("error", err))
		}
	}()

	c.wg.Wait()

	return nil
}
