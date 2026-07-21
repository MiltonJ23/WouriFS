/*
 * Cluster observability types — hand-written to avoid proto regeneration.
 * These implement the same proto message interface as generated code.
 */
package pb

import (
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/runtime/protoimpl"
)

type ListNodesRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields
}

func (*ListNodesRequest) ProtoMessage()                               {}
func (*ListNodesRequest) Reset()                                      { *((*ListNodesRequest)(nil)) = ListNodesRequest{} }
func (x *ListNodesRequest) ProtoReflect() protoreflect.Message        { return nil }
func (*ListNodesRequest) Descriptor() ([]byte, []int)                 { return nil, nil }

type NodeInfo struct {
	state              protoimpl.MessageState
	sizeCache          protoimpl.SizeCache
	unknownFields      protoimpl.UnknownFields
	NodeId             string   `protobuf:"bytes,1,opt,name=node_id,json=nodeId,proto3" json:"node_id,omitempty"`
	Roles              []string `protobuf:"bytes,2,rep,name=roles,proto3" json:"roles,omitempty"`
	TailscaleIp        string   `protobuf:"bytes,3,opt,name=tailscale_ip,json=tailscaleIp,proto3" json:"tailscale_ip,omitempty"`
	Port               int32    `protobuf:"varint,4,opt,name=port,proto3" json:"port,omitempty"`
	Available          bool     `protobuf:"varint,5,opt,name=available,proto3" json:"available,omitempty"`
	StorageUsedBytes   int64    `protobuf:"varint,6,opt,name=storage_used_bytes,json=storageUsedBytes,proto3" json:"storage_used_bytes,omitempty"`
	StorageTotalBytes  int64    `protobuf:"varint,7,opt,name=storage_total_bytes,json=storageTotalBytes,proto3" json:"storage_total_bytes,omitempty"`
	FuseSessions       int32    `protobuf:"varint,8,opt,name=fuse_sessions,json=fuseSessions,proto3" json:"fuse_sessions,omitempty"`
	LastHeartbeatUnix  int64    `protobuf:"varint,9,opt,name=last_heartbeat_unix,json=lastHeartbeatUnix,proto3" json:"last_heartbeat_unix,omitempty"`
}

func (*NodeInfo) ProtoMessage()                               {}
func (*NodeInfo) Reset()                                      { *((*NodeInfo)(nil)) = NodeInfo{} }
func (x *NodeInfo) ProtoReflect() protoreflect.Message        { return nil }
func (*NodeInfo) Descriptor() ([]byte, []int)                 { return nil, nil }

type ListNodesResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields
	Nodes         []*NodeInfo `protobuf:"bytes,1,rep,name=nodes,proto3" json:"nodes,omitempty"`
	RaftLeaderId  string      `protobuf:"bytes,2,opt,name=raft_leader_id,json=raftLeaderId,proto3" json:"raft_leader_id,omitempty"`
}

func (*ListNodesResponse) ProtoMessage()                               {}
func (*ListNodesResponse) Reset()                                      { *((*ListNodesResponse)(nil)) = ListNodesResponse{} }
func (x *ListNodesResponse) ProtoReflect() protoreflect.Message        { return nil }
func (*ListNodesResponse) Descriptor() ([]byte, []int)                 { return nil, nil }
