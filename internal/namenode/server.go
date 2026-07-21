package namenode

import (
	"context"
	"time"

	pb "github.com/MiltonJ23/WouriFS/api/gen/v1/namenode"
	"github.com/MiltonJ23/WouriFS/internal/domain"
	domainnn "github.com/MiltonJ23/WouriFS/internal/domain/namenode"
	"github.com/MiltonJ23/WouriFS/internal/observability"
	"github.com/MiltonJ23/WouriFS/internal/transport/grpc/interceptor"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// NameNodeServer implements pb.NameNodeServiceServer.
type NameNodeServer struct {
	pb.UnimplementedNameNodeServiceServer
	store    *MetadataStore
	registry *domainnn.InMemoryDataNodeRegistry
	wal      *WAL
	log      *observability.Logger
}

// NewNameNodeServer creates the gRPC handler. If logger is nil, a silent test logger is used.
func NewNameNodeServer(store *MetadataStore, registry *domainnn.InMemoryDataNodeRegistry, wal *WAL, logger *observability.Logger) *NameNodeServer {
	if logger == nil {
		logger = observability.NewTestLogger()
	}
	return &NameNodeServer{store: store, registry: registry, wal: wal, log: logger}
}

// --- DataNode lifecycle ---

func (s *NameNodeServer) RegisterDataNode(ctx context.Context, req *pb.RegisterDataNodeRequest) (*pb.RegisterDataNodeResponse, error) {
	start, _ := s.log.OpStart("RegisterDataNode", req.DatanodeId)
	defer func() { s.log.OpEnd(start, "RegisterDataNode", req.DatanodeId, nil, 0) }()

	node := &domainnn.DataNodeStatus{
		ID:                req.DatanodeId,
		Address:           req.Address,
		TotalStorageBytes: req.TotalStorageBytes,
		FreeStorageBytes:  req.FreeStorageBytes,
		LastHeartbeat:     time.Now(),
		IsAvailable:       true,
	}
	if err := s.registry.Register(node); err != nil {
		return nil, status.Errorf(codes.Internal, "register: %v", err)
	}
	s.log.Info("datanode_registered", "id", req.DatanodeId, "addr", req.Address)
	return &pb.RegisterDataNodeResponse{}, nil
}

func (s *NameNodeServer) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	err := s.registry.UpdateHeartbeat(req.DatanodeId, req.FreeStorageBytes, req.ActiveConnections)
	if err != nil {
		s.log.Debug("heartbeat_unknown_node", "id", req.DatanodeId)
		return &pb.HeartbeatResponse{Acknowledged: false, RequireReregistration: true}, nil
	}
	return &pb.HeartbeatResponse{Acknowledged: true}, nil
}

// --- File operations ---

func (s *NameNodeServer) CreateFile(ctx context.Context, req *pb.CreateFileRequest) (*pb.CreateFileResponse, error) {
	start, _ := s.log.OpStart("CreateFile", req.Path)
	payload, err := s.auth(ctx)
	if err != nil {
		s.log.OpEnd(start, "CreateFile", req.Path, err, 0)
		return nil, err
	}
	if err := s.store.CheckNamespace(payload, req.Path); err != nil {
		s.log.OpEnd(start, "CreateFile", req.Path, err, 0)
		return nil, status.Error(codes.PermissionDenied, err.Error())
	}

	mode := req.Mode
	if mode == 0 {
		mode = 0644
	}
	fm, err := s.store.CreateFile(req.Path, mode)
	if err != nil {
		s.log.OpEnd(start, "CreateFile", req.Path, err, 0)
		if err == ErrFileExists {
			return nil, status.Error(codes.AlreadyExists, err.Error())
		}
		return nil, status.Errorf(codes.Internal, "create: %v", err)
	}

	s.walAppend(WALEntry{Op: "create_file", Path: req.Path, FileID: fm.FileID, Mode: fm.Mode, Size: 0})
	s.log.OpEnd(start, "CreateFile", req.Path, nil, 0)
	return &pb.CreateFileResponse{FileId: fm.FileID}, nil
}

func (s *NameNodeServer) DeleteFile(ctx context.Context, req *pb.DeleteFileRequest) (*pb.DeleteFileResponse, error) {
	start, _ := s.log.OpStart("DeleteFile", req.Path)
	payload, err := s.auth(ctx)
	if err != nil {
		s.log.OpEnd(start, "DeleteFile", req.Path, err, 0)
		return nil, err
	}
	if err := s.store.CheckNamespace(payload, req.Path); err != nil {
		s.log.OpEnd(start, "DeleteFile", req.Path, err, 0)
		return nil, status.Error(codes.PermissionDenied, err.Error())
	}

	if err := s.store.DeleteFile(req.Path); err != nil {
		s.log.OpEnd(start, "DeleteFile", req.Path, err, 0)
		if err == ErrFileNotFound {
			return nil, status.Error(codes.NotFound, err.Error())
		}
		return nil, status.Errorf(codes.Internal, "delete: %v", err)
	}

	s.walAppend(WALEntry{Op: "delete_file", Path: req.Path})
	s.log.OpEnd(start, "DeleteFile", req.Path, nil, 0)
	return &pb.DeleteFileResponse{}, nil
}

func (s *NameNodeServer) LookupFile(ctx context.Context, req *pb.LookupFileRequest) (*pb.LookupFileResponse, error) {
	start, _ := s.log.OpStart("LookupFile", req.Path)
	payload, err := s.auth(ctx)
	if err != nil {
		s.log.OpEnd(start, "LookupFile", req.Path, err, 0)
		return nil, err
	}
	if err := s.store.CheckNamespace(payload, req.Path); err != nil {
		s.log.OpEnd(start, "LookupFile", req.Path, err, 0)
		return nil, status.Error(codes.PermissionDenied, err.Error())
	}

	fm, err := s.store.GetFile(req.Path)
	if err != nil {
		s.log.OpEnd(start, "LookupFile", req.Path, err, 0)
		return nil, status.Error(codes.NotFound, err.Error())
	}

	chunks := make([]*pb.ChunkLocation, len(fm.Chunks))
	for i, c := range fm.Chunks {
		addr := ""
		if len(c.Replicas) > 0 {
			addr = c.Replicas[0]
		}
		chunks[i] = &pb.ChunkLocation{ChunkId: c.ChunkID, DatanodeAddress: addr}
	}

	s.log.OpEnd(start, "LookupFile", req.Path, nil, fm.Size)
	return &pb.LookupFileResponse{
		FileId:    fm.FileID,
		Chunks:    chunks,
		SizeBytes: fm.Size,
		Mode:      fm.Mode,
		MtimeUnix: fm.Mtime.Unix(),
	}, nil
}

func (s *NameNodeServer) ListDirectory(ctx context.Context, req *pb.ListDirectoryRequest) (*pb.ListDirectoryResponse, error) {
	start, _ := s.log.OpStart("ListDirectory", req.Path)
	payload, err := s.auth(ctx)
	if err != nil {
		s.log.OpEnd(start, "ListDirectory", req.Path, err, 0)
		return nil, err
	}
	path := req.Path
	if path == "" {
		path = payload.Namespace
	}
	if err := s.store.CheckNamespace(payload, path); err != nil {
		s.log.OpEnd(start, "ListDirectory", req.Path, err, 0)
		return nil, status.Error(codes.PermissionDenied, err.Error())
	}

	entries := s.store.ListDirectory(path)
	pbEntries := make([]*pb.DirEntry, len(entries))
	for i, e := range entries {
		pbEntries[i] = &pb.DirEntry{
			Name:       e.Name,
			IsDir:      e.IsDir,
			SizeBytes:  e.Size,
			Mode:       e.Mode,
			MtimeUnix:  e.Mtime.Unix(),
		}
	}
	s.log.OpEnd(start, "ListDirectory", req.Path, nil, int64(len(pbEntries)))
	return &pb.ListDirectoryResponse{Entries: pbEntries}, nil
}

func (s *NameNodeServer) MakeDirectory(ctx context.Context, req *pb.MakeDirectoryRequest) (*pb.MakeDirectoryResponse, error) {
	start, _ := s.log.OpStart("MakeDirectory", req.Path)
	payload, err := s.auth(ctx)
	if err != nil {
		s.log.OpEnd(start, "MakeDirectory", req.Path, err, 0)
		return nil, err
	}
	if err := s.store.CheckNamespace(payload, req.Path); err != nil {
		s.log.OpEnd(start, "MakeDirectory", req.Path, err, 0)
		return nil, status.Error(codes.PermissionDenied, err.Error())
	}

	if err := s.store.MakeDir(req.Path, req.Mode); err != nil {
		s.log.OpEnd(start, "MakeDirectory", req.Path, err, 0)
		if err == ErrFileExists {
			return nil, status.Error(codes.AlreadyExists, err.Error())
		}
		return nil, status.Errorf(codes.Internal, "mkdir: %v", err)
	}

	s.walAppend(WALEntry{Op: "mkdir", Path: req.Path})
	s.log.OpEnd(start, "MakeDirectory", req.Path, nil, 0)
	return &pb.MakeDirectoryResponse{}, nil
}

func (s *NameNodeServer) RemoveDirectory(ctx context.Context, req *pb.RemoveDirectoryRequest) (*pb.RemoveDirectoryResponse, error) {
	start, _ := s.log.OpStart("RemoveDirectory", req.Path)
	payload, err := s.auth(ctx)
	if err != nil {
		s.log.OpEnd(start, "RemoveDirectory", req.Path, err, 0)
		return nil, err
	}
	if err := s.store.CheckNamespace(payload, req.Path); err != nil {
		s.log.OpEnd(start, "RemoveDirectory", req.Path, err, 0)
		return nil, status.Error(codes.PermissionDenied, err.Error())
	}

	if err := s.store.RemoveDir(req.Path); err != nil {
		s.log.OpEnd(start, "RemoveDirectory", req.Path, err, 0)
		if err == ErrFileNotFound {
			return nil, status.Error(codes.NotFound, err.Error())
		}
		return nil, status.Errorf(codes.Internal, "rmdir: %v", err)
	}

	s.walAppend(WALEntry{Op: "rmdir", Path: req.Path})
	s.log.OpEnd(start, "RemoveDirectory", req.Path, nil, 0)
	return &pb.RemoveDirectoryResponse{}, nil
}

func (s *NameNodeServer) RenameFile(ctx context.Context, req *pb.RenameFileRequest) (*pb.RenameFileResponse, error) {
	start, _ := s.log.OpStart("RenameFile", req.OldPath+"→"+req.NewPath)
	payload, err := s.auth(ctx)
	if err != nil {
		s.log.OpEnd(start, "RenameFile", req.OldPath, err, 0)
		return nil, err
	}
	if err := s.store.CheckNamespace(payload, req.OldPath); err != nil {
		s.log.OpEnd(start, "RenameFile", req.OldPath, err, 0)
		return nil, status.Error(codes.PermissionDenied, err.Error())
	}
	if err := s.store.CheckNamespace(payload, req.NewPath); err != nil {
		s.log.OpEnd(start, "RenameFile", req.OldPath, err, 0)
		return nil, status.Error(codes.PermissionDenied, err.Error())
	}

	if err := s.store.Rename(req.OldPath, req.NewPath); err != nil {
		s.log.OpEnd(start, "RenameFile", req.OldPath, err, 0)
		if err == ErrFileNotFound {
			return nil, status.Error(codes.NotFound, err.Error())
		}
		return nil, status.Errorf(codes.Internal, "rename: %v", err)
	}

	s.walAppend(WALEntry{Op: "rename", Path: req.OldPath, OldPath: req.OldPath, NewPath: req.NewPath})
	s.log.OpEnd(start, "RenameFile", req.OldPath, nil, 0)
	return &pb.RenameFileResponse{}, nil
}

func (s *NameNodeServer) StatFile(ctx context.Context, req *pb.StatFileRequest) (*pb.StatFileResponse, error) {
	start, _ := s.log.OpStart("StatFile", req.Path)
	payload, err := s.auth(ctx)
	if err != nil {
		s.log.OpEnd(start, "StatFile", req.Path, err, 0)
		return nil, err
	}
	if err := s.store.CheckNamespace(payload, req.Path); err != nil {
		s.log.OpEnd(start, "StatFile", req.Path, err, 0)
		return nil, status.Error(codes.PermissionDenied, err.Error())
	}

	fm, err := s.store.GetFile(req.Path)
	if err != nil {
		s.log.OpEnd(start, "StatFile", req.Path, err, 0)
		return nil, status.Error(codes.NotFound, err.Error())
	}

	s.log.OpEnd(start, "StatFile", req.Path, nil, fm.Size)
	mode := fm.Mode
	if mode == 0 && fm.IsDir {
		mode = 0755
	}
	if mode == 0 {
		mode = 0644
	}
	return &pb.StatFileResponse{
		FileId:     fm.FileID,
		Name:       fm.Path,
		SizeBytes:  fm.Size,
		Mode:       mode,
		IsDir:      fm.IsDir,
		MtimeUnix:  fm.Mtime.Unix(),
		CtimeUnix:  fm.Ctime.Unix(),
	}, nil
}

func (s *NameNodeServer) TruncateFile(ctx context.Context, req *pb.TruncateFileRequest) (*pb.TruncateFileResponse, error) {
	start, _ := s.log.OpStart("TruncateFile", req.Path)
	payload, err := s.auth(ctx)
	if err != nil {
		s.log.OpEnd(start, "TruncateFile", req.Path, err, 0)
		return nil, err
	}
	if err := s.store.CheckNamespace(payload, req.Path); err != nil {
		s.log.OpEnd(start, "TruncateFile", req.Path, err, 0)
		return nil, status.Error(codes.PermissionDenied, err.Error())
	}

	if err := s.store.TruncateFile(req.Path, req.SizeBytes); err != nil {
		s.log.OpEnd(start, "TruncateFile", req.Path, err, 0)
		return nil, status.Error(codes.NotFound, err.Error())
	}

	s.walAppend(WALEntry{Op: "truncate_file", Path: req.Path, Size: req.SizeBytes})
	s.log.OpEnd(start, "TruncateFile", req.Path, nil, req.SizeBytes)
	return &pb.TruncateFileResponse{}, nil
}

// --- Chunk allocation ---

func (s *NameNodeServer) AllocateChunk(ctx context.Context, req *pb.AllocateChunkRequest) (*pb.AllocateChunkResponse, error) {
	start, _ := s.log.OpStart("AllocateChunk", req.FileId)
	payload, err := s.auth(ctx)
	if err != nil {
		s.log.OpEnd(start, "AllocateChunk", req.FileId, err, 0)
		return nil, err
	}

	// Resolve file path from FileID so we can enforce namespace isolation.
	// The caller's JWT must authorize writes to the owning path.
	fm, err := s.store.GetFileByID(req.FileId)
	if err != nil {
		s.log.OpEnd(start, "AllocateChunk", req.FileId, err, 0)
		return nil, status.Error(codes.NotFound, "file not found")
	}
	if err := s.store.CheckNamespace(payload, fm.Path); err != nil {
		s.log.OpEnd(start, "AllocateChunk", req.FileId, err, 0)
		return nil, status.Error(codes.PermissionDenied, err.Error())
	}

	rf := req.ReplicationFactor
	if rf <= 0 {
		rf = s.store.ReplicationFactor()
	}

	available := s.registry.GetAllAvailable()
	if len(available) < int(rf) {
		err := status.Errorf(codes.ResourceExhausted, "need %d datanodes, only %d available", rf, len(available))
		s.log.OpEnd(start, "AllocateChunk", req.FileId, err, 0)
		return nil, err
	}

	addrs := make([]string, 0, rf)
	for i := int32(0); i < rf; i++ {
		addrs = append(addrs, available[i].Address)
	}

	chunkID := newID()

	if err := s.store.AddChunk(req.FileId, chunkID, addrs); err != nil {
		s.log.OpEnd(start, "AllocateChunk", req.FileId, err, 0)
		return nil, status.Errorf(codes.Internal, "add chunk: %v", err)
	}

	s.walAppend(WALEntry{Op: "add_chunk", FileID: req.FileId, ChunkID: chunkID, Replicas: addrs})
	s.log.OpEnd(start, "AllocateChunk", req.FileId, nil, int64(len(addrs)))
	return &pb.AllocateChunkResponse{ChunkId: chunkID, DatanodeAddresses: addrs}, nil
}

// --- Helpers ---

func (s *NameNodeServer) auth(ctx context.Context) (*domain.TokenPayload, error) {
	return interceptor.PayloadFromContext(ctx)
}

// ListNodes returns the current cluster topology (FR-ADM-008).
func (s *NameNodeServer) ListNodes(ctx context.Context) *pb.ListNodesResponse {
	nodes := s.registry.ListStatus()
	pbNodes := make([]*pb.NodeInfo, len(nodes))
	for i, n := range nodes {
		pbNodes[i] = &pb.NodeInfo{
			NodeId:            n.ID,
			Roles:             n.Roles,
			TailscaleIp:       n.Address,
			Port:              n.Port,
			Available:         n.IsAvailable,
			StorageUsedBytes:  n.StorageUsed,
			StorageTotalBytes: n.TotalStorage,
			FuseSessions:      n.FuseSessions,
			LastHeartbeatUnix: n.LastHeartbeat.Unix(),
		}
	}
	return &pb.ListNodesResponse{
		Nodes:        pbNodes,
		RaftLeaderId: "", // set when Raft HA is wired
	}
}

func (s *NameNodeServer) walAppend(e WALEntry) {
	if s.wal == nil {
		return
	}
	if err := s.wal.Append(e); err != nil {
		s.log.Warn("wal_append_failed", "op", e.Op, "path", e.Path, "error", err)
	}
}
