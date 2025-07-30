package client_pool

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/mochivi/distributed-file-system/internal/clients"
	"github.com/mochivi/distributed-file-system/internal/common"
)

type ClientPool interface {
	GetClientWithRetry(clientConnectionFunc clientConnectionFunc) (clients.IDataNodeClient, any, error)
	GetRemoveClientWithRetry(clientConnectionFunc clientConnectionFunc) (clients.IDataNodeClient, any, error)
	Len() int
	Close()
}

type ClientPoolFactory func(nodes []*common.NodeInfo) (ClientPool, error)

// hardcoded to return streaming session id, in the future we might want to return the server response itself by using some abstraction over it?
type clientConnectionFunc func(client clients.IDataNodeClient) (bool, any, error)

// rotatingClientPool is a client pool that rotates through the nodes in a round-robin fashion
// Useful when we don't care about specific nodes, but we want to distribute the load evenly
type rotatingClientPool struct {
	index   int
	clients []clients.IDataNodeClient
	mu      sync.RWMutex
}

func NewRotatingClientPool(nodes []*common.NodeInfo) (*rotatingClientPool, error) {
	nodeClients := make([]clients.IDataNodeClient, 0, len(nodes))
	for _, node := range nodes {
		client, err := clients.NewDataNodeClient(node)
		if err != nil {
			return nil, fmt.Errorf("failed to create client: %w", err)
		}
		nodeClients = append(nodeClients, client)
	}
	return &rotatingClientPool{
		clients: nodeClients,
	}, nil
}

func (c *rotatingClientPool) GetRemoveClientWithRetry(clientConnectionFunc clientConnectionFunc) (clients.IDataNodeClient, any, error) {
	c.mu.Lock()
	if len(c.clients) == 0 {
		c.mu.Unlock()
		return nil, nil, errors.New("empty pool")
	}

	defer func() {
		c.removeClient() // by removing the item at the current index, it means the index automatically points to the next item
		c.mu.Unlock()
	}()

	return c.getClientWithRetry(clientConnectionFunc)
}

func (c *rotatingClientPool) GetClientWithRetry(clientConnectionFunc clientConnectionFunc) (clients.IDataNodeClient, any, error) {
	c.mu.Lock()
	if len(c.clients) == 0 {
		c.mu.Unlock()
		return nil, nil, errors.New("empty pool")
	}

	defer func() {
		c.rotateClient() // rotate client no matter what happened: success, error or refusal
		c.mu.Unlock()
	}()
	return c.getClientWithRetry(clientConnectionFunc)
}

// Internal reusable function to get a client with retry, does not lock the mutex as it should already be locked by the caller
func (c *rotatingClientPool) getClientWithRetry(clientConnectionFunc clientConnectionFunc) (clients.IDataNodeClient, any, error) {
	// based on the starting index, create a new slice that starts from it
	orderedClients := append(c.clients[c.index:], c.clients[:c.index]...)

	for _, client := range orderedClients { // try all clients once

		// same client retry loop
		for i := 1; i <= 3; i++ {
			accepted, response, err := clientConnectionFunc(client)
			if err != nil { // client connection error, retry the same client
				time.Sleep(time.Duration(i) * 500 * time.Millisecond) // Exponential backoff on connection attempts
				continue
			}

			if !accepted { // if client refused connection connection, we don't retry it
				break
			}

			// success, return client, ensure next call to getClient tries the next node
			return client, response, nil
		}
	}

	return nil, nil, fmt.Errorf("failed to get client")
}

// Internal function, mutex should be locked by caller
func (c *rotatingClientPool) rotateClient() {
	c.index = (c.index + 1) % len(c.clients)
}

// private function, does not lock the mutex as it should already be locked by the caller
func (c *rotatingClientPool) removeClient() {
	c.clients = append(c.clients[:c.index], c.clients[c.index+1:]...)
}

func (c *rotatingClientPool) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.clients)
}

func (c *rotatingClientPool) Close() {
	for _, client := range c.clients {
		client.Close()
	}
}
