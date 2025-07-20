package cluster

import "log/slog"

func (c *CoordinatorNodeAgent) Run() error {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		if err := c.controllers.GarbageCollector.Run(); err != nil {
			c.logger.Error("DeletedFiles garbage collector loop failed", slog.Any("error", err))
		}
	}()

	c.wg.Wait()

	return nil
}
