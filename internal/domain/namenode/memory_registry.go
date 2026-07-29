package namenode

import (
	"errors"
	"sync"
	"time"
)

var (
	ErrNodeNotFound = errors.New("datanode not found in registry")
	ErrInvalidNode  = errors.New("invalid datanode data")
)

type InMemoryDataNodeRegistry struct {
	mu    sync.RWMutex
	nodes map[string]*DataNodeStatus
}

func NewInMemoryDataNodeRegistry() *InMemoryDataNodeRegistry {
	return &InMemoryDataNodeRegistry{
		nodes: make(map[string]*DataNodeStatus),
	}
}

func (i *InMemoryDataNodeRegistry) Register(node *DataNodeStatus) error {
	if node == nil || node.ID == "" || node.Address == "" {
		return ErrInvalidNode
	}

	i.mu.Lock()
	defer i.mu.Unlock()

	nodeCopy := *node
	nodeCopy.LastHeartbeat = time.Now()
	nodeCopy.IsAvailable = true
	nodeCopy.StorageUsed = node.TotalStorageBytes - node.FreeStorageBytes
	nodeCopy.TotalStorage = node.TotalStorageBytes
	i.nodes[nodeCopy.ID] = &nodeCopy

	return nil
}

func (i *InMemoryDataNodeRegistry) UpdateHeartbeat(id string, freeStorage int64, activeConnections int32) error {
	i.mu.Lock()
	defer i.mu.Unlock()

	node, exists := i.nodes[id]
	if !exists {
		return ErrNodeNotFound
	}
	node.FreeStorageBytes = freeStorage
	node.ActiveConnection = activeConnections
	node.LastHeartbeat = time.Now()

	node.IsAvailable = true

	return nil
}

func (i *InMemoryDataNodeRegistry) MarkUnavailable(id string) error {
	i.mu.Lock()
	defer i.mu.Unlock()

	node, exists := i.nodes[id]
	if !exists {
		return ErrNodeNotFound
	}

	node.IsAvailable = false
	return nil
}

func (i *InMemoryDataNodeRegistry) GetAllAvailable() []*DataNodeStatus {
	i.mu.RLock()
	defer i.mu.RUnlock()

	var availableNodes []*DataNodeStatus

	for _, node := range i.nodes {
		if node.IsAvailable {
			nodeCopy := *node
			availableNodes = append(availableNodes, &nodeCopy)
		}
	}

	return availableNodes
}

func (i *InMemoryDataNodeRegistry) GetAll() []*DataNodeStatus {
	i.mu.RLock()
	defer i.mu.RUnlock()

	var allNodes []*DataNodeStatus
	for _, node := range i.nodes {
		nodeCopy := *node
		allNodes = append(allNodes, &nodeCopy)
	}
	return allNodes
}

// ListStatus returns all nodes with their full status for the admin dashboard.
func (i *InMemoryDataNodeRegistry) ListStatus() []*DataNodeStatus {
	return i.GetAll()
}
