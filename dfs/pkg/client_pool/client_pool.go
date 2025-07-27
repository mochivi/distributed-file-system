package client_pool

import (
	"fmt"
	"sync"
	"time"

	"github.com/mochivi/distributed-file-system/internal/clients"
	"github.com/mochivi/distributed-file-system/internal/common"
)

type ClientPool interface {
	GetClient() clients.IDataNodeClient
	GetClientWithRetry(clientConnectionFunc clientConnectionFunc) (clients.IDataNodeClient, string, error)
	GetRemoveClientWithRetry(clientConnectionFunc clientConnectionFunc) (clients.IDataNodeClient, string, error)
	Len() int
	Close()
}

type ClientPoolFactory func(nodes []*common.NodeInfo) (ClientPool, error)

// hardcoded to return streaming session id, in the future we might want to return the server response itself by using some abstraction over it?
type clientConnectionFunc func(client clients.IDataNodeClient) (bool, string, error)

// rotatingClientPool is a client pool that rotates through the nodes in a round-robin fashion
// Useful when we don't care about specific nodes, but we want to distribute the load evenly
type rotatingClientPool struct {
	index   int
	clients []clients.IDataNodeClient
	mu      sync.Mutex
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

func (c *rotatingClientPool) GetClient() clients.IDataNodeClient {
	c.mu.Lock()
	defer c.mu.Unlock()

	client := c.clients[c.index]
	c.rotateClient() // next request to GetClient should return a different node
	return client
}

func (c *rotatingClientPool) GetRemoveClientWithRetry(clientConnectionFunc clientConnectionFunc) (clients.IDataNodeClient, string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	client, streamingSessionID, err := c.getClientWithRetry(clientConnectionFunc)
	if err != nil {
		return nil, "", err
	}

	c.removeClient(client.Node().ID)
	return client, streamingSessionID, nil
}

func (c *rotatingClientPool) GetClientWithRetry(clientConnectionFunc clientConnectionFunc) (clients.IDataNodeClient, string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.getClientWithRetry(clientConnectionFunc)
}

// Internal reusable function to get a client with retry, does not lock the mutex as it should already be locked by the caller
func (c *rotatingClientPool) getClientWithRetry(clientConnectionFunc clientConnectionFunc) (clients.IDataNodeClient, string, error) {
	for range len(c.clients) { // try all clients once
		client := c.clients[c.index]

		// same client retry loop
		for i := 1; i <= 3; i++ {
			accepted, streamingSessionID, err := clientConnectionFunc(client)
			if err != nil { // client connection error, retry the same client
				time.Sleep(time.Duration(i) * 500 * time.Millisecond) // Exponential backoff on connection attempts
				continue
			}

			if !accepted { // if client refused connection connection, we don't retry it
				c.rotateClient()
				break
			}

			// success, return client, ensure next call to getClient tries the next node
			c.rotateClient()
			return client, streamingSessionID, nil
		}
	}

	return nil, "", fmt.Errorf("failed to get client")
}

// Internal function, mutex should be locked by caller
func (c *rotatingClientPool) rotateClient() {
	c.index = (c.index + 1) % len(c.clients)
}

// private function, does not lock the mutex as it should already be locked by the caller
func (c *rotatingClientPool) removeClient(nodeID string) {
	// Iterate backwards to avoid index issues when removing elements
	for i := len(c.clients) - 1; i >= 0; i-- {
		if c.clients[i].Node().ID == nodeID {
			c.clients = append(c.clients[:i], c.clients[i+1:]...)

			// Adjust rotation index if necessary
			if c.index >= len(c.clients) || c.index == 0 {
				c.index = 0
			} else {
				c.index-- // Always needed due to the rotation that happened before removal
			}
			break // Remove only the first occurrence
		}
	}
}

func (c *rotatingClientPool) Len() int {
	return len(c.clients)
}

func (c *rotatingClientPool) Close() {
	for _, client := range c.clients {
		client.Close()
	}
}
