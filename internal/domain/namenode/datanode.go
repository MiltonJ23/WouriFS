package namenode

import "time"

// DataNodeStatus refers to the live status of a datanode
type DataNodeStatus struct {
	ID                string    `json:"id"`
	Address           string    `json:"address"`
	TotalStorageBytes int64     `json:"total_storage_bytes"`
	FreeStorageBytes  int64     `json:"free_storage_bytes"`
	ActiveConnection  int32     `json:"active_connection"`
	LastHeartbeat     time.Time `json:"last_heartbeat"`
	IsAvailable       bool      `json:"is_available"`
}

type DataNodeRegistry interface {
	Register(node *DataNodeStatus) error
	UpdateHeartbeat(id string, freeStorage int64, activeConnections int32) error
	MarkUnavailable(id string) error
	GetAllAvailable() []*DataNodeStatus
	GetAll() []*DataNodeStatus
}
