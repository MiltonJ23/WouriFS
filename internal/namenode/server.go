package namenode

import (
	"context"
	"log"
	"time"

	pb "github.com/MiltonJ23/WouriFS/api/gen/v1/namenode"
	"github.com/MiltonJ23/WouriFS/internal/domain"
	domainnn "github.com/MiltonJ23/WouriFS/internal/domain/namenode"
	"github.com/MiltonJ23/WouriFS/internal/transport/grpc/interceptor"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// NameNodeServer implements pb.NameNodeServiceServer (FR-N-004).
// It is the gRPC handler wired into the secure gRPC server.
type NameNodeServer struct {
	pb.UnimplementedNameNodeServiceServer
	store    *MetadataStore
	registry *domainnn.InMemoryDataNodeRegistry
	wal      *WAL
}

// NewNameNodeServer creates the gRPC handler.
func NewNameNodeServer(store *MetadataStore, registry *domainnn.InMemoryDataNodeRegistry, wal *WAL) *NameNodeServer {
	return &NameNodeServer{store: store, registry: registry, wal: wal}
}

// --- DataNode lifecycle ---

// RegisterDataNode handles Datanode registration (FR-D-007).
func (s *NameNodeServer) RegisterDataNode(ctx context.Context, req *pb.RegisterDataNodeRequest) (*pb.RegisterDataNodeResponse, error) {
	node := &domainnn.DataNodeStatus{
		ID:                req.DatanodeId,
		Address:           req.Address,
		TotalStorageBytes: req.TotalStorageBytes,
		FreeStorageBytes:  req.FreeStorageBytes,
		LastHeartbeat:     time.Now(),
		IsAvailable:       true,
	}
	if err := s.registry.Register(node); err != nil {
		return nil, status.Errorf(codes.Internal, "register failed: %v", err)
	}
	log.Printf("datanode registered: %s at %s", req.DatanodeId, req.Address)
	return &pb.RegisterDataNodeResponse{}, nil
}

// Heartbeat updates liveness for a datanode (FR-D-003).
func (s *NameNodeServer) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	err := s.registry.UpdateHeartbeat(req.DatanodeId, req.FreeStorageBytes, req.ActiveConnections)
	if err != nil {
		// Amnesia recovery: unknown node → ask for re-registration
		return &pb.HeartbeatResponse{
			Acknowledged:           false,
			RequireReregistration: true,
		}, nil
	}
	return &pb.HeartbeatResponse{Acknowledged: true}, nil
}

// --- File operations with namespace enforcement (FR-N-007) ---

func (s *NameNodeServer) CreateFile(ctx context.Context, req *pb.CreateFileRequest) (*pb.CreateFileResponse, error) {
	payload, err := s.auth(ctx)
	if err != nil {
		return nil, err
	}
	if err := s.store.CheckNamespace(payload, req.Path); err != nil {
		return nil, status.Error(codes.PermissionDenied, err.Error())
	}

	fm, err := s.store.CreateFile(req.Path)
	if err != nil {
		if err == ErrFileExists {
			return nil, status.Error(codes.AlreadyExists, err.Error())
		}
		return nil, status.Errorf(codes.Internal, "create file: %v", err)
	}

	if s.wal != nil {
		s.wal.Append(WALEntry{Op: "create_file", Path: req.Path, FileID: fm.FileID})
	}

	return &pb.CreateFileResponse{FileId: fm.FileID}, nil
}

func (s *NameNodeServer) DeleteFile(ctx context.Context, req *pb.DeleteFileRequest) (*pb.DeleteFileResponse, error) {
	payload, err := s.auth(ctx)
	if err != nil {
		return nil, err
	}
	if err := s.store.CheckNamespace(payload, req.Path); err != nil {
		return nil, status.Error(codes.PermissionDenied, err.Error())
	}

	if err := s.store.DeleteFile(req.Path); err != nil {
		if err == ErrFileNotFound {
			return nil, status.Error(codes.NotFound, err.Error())
		}
		return nil, status.Errorf(codes.Internal, "delete file: %v", err)
	}

	if s.wal != nil {
		s.wal.Append(WALEntry{Op: "delete_file", Path: req.Path})
	}

	return &pb.DeleteFileResponse{}, nil
}

func (s *NameNodeServer) LookupFile(ctx context.Context, req *pb.LookupFileRequest) (*pb.LookupFileResponse, error) {
	payload, err := s.auth(ctx)
	if err != nil {
		return nil, err
	}
	if err := s.store.CheckNamespace(payload, req.Path); err != nil {
		return nil, status.Error(codes.PermissionDenied, err.Error())
	}

	fm, err := s.store.GetFile(req.Path)
	if err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}

	chunks := make([]*pb.ChunkLocation, len(fm.Chunks))
	for i, c := range fm.Chunks {
		replicas := make([]*pb.ChunkLocation, len(c.Replicas))
		for j, addr := range c.Replicas {
			replicas[j] = &pb.ChunkLocation{ChunkId: c.ChunkID, DatanodeAddress: addr}
		}
		chunks[i] = &pb.ChunkLocation{ChunkId: c.ChunkID, DatanodeAddress: c.Replicas[0]}
		_ = replicas // keep in scope for future use
	}
	return &pb.LookupFileResponse{FileId: fm.FileID, Chunks: chunks}, nil
}

func (s *NameNodeServer) ListDirectory(ctx context.Context, req *pb.ListDirectoryRequest) (*pb.ListDirectoryResponse, error) {
	payload, err := s.auth(ctx)
	if err != nil {
		return nil, err
	}
	path := req.Path
	if path == "" {
		path = payload.Namespace // root of user's namespace
	}
	if err := s.store.CheckNamespace(payload, path); err != nil {
		return nil, status.Error(codes.PermissionDenied, err.Error())
	}

	entries := s.store.ListDirectory(path)
	pbEntries := make([]*pb.DirEntry, len(entries))
	for i, e := range entries {
		pbEntries[i] = &pb.DirEntry{Name: e.Name, IsDir: e.IsDir, SizeBytes: e.Size}
	}
	return &pb.ListDirectoryResponse{Entries: pbEntries}, nil
}

// --- Chunk allocation (FR-N-006: skip unavailable nodes) ---

func (s *NameNodeServer) AllocateChunk(ctx context.Context, req *pb.AllocateChunkRequest) (*pb.AllocateChunkResponse, error) {
	rf := req.ReplicationFactor
	if rf <= 0 {
		rf = s.store.ReplicationFactor()
	}

	// Select RF available datanodes (FR-N-006: never use UNAVAILABLE)
	available := s.registry.GetAllAvailable()
	if len(available) < int(rf) {
		return nil, status.Errorf(codes.ResourceExhausted,
			"need %d datanodes, only %d available", rf, len(available))
	}

	addrs := make([]string, 0, rf)
	for i := int32(0); i < rf; i++ {
		addrs = append(addrs, available[i].Address)
	}

	chunkID := newID()

	// Record chunk in metadata
	if err := s.store.AddChunk(req.FileId, chunkID, addrs); err != nil {
		return nil, status.Errorf(codes.Internal, "add chunk: %v", err)
	}

	if s.wal != nil {
		s.wal.Append(WALEntry{
			Op:       "add_chunk",
			FileID:   req.FileId,
			ChunkID:  chunkID,
			Replicas: addrs,
		})
	}

	return &pb.AllocateChunkResponse{
		ChunkId:           chunkID,
		DatanodeAddresses: addrs,
	}, nil
}

// auth extracts the token payload from context injected by AuthInterceptor (FR-N-007).
func (s *NameNodeServer) auth(ctx context.Context) (*domain.TokenPayload, error) {
	payload, err := interceptor.PayloadFromContext(ctx)
	if err != nil {
		return nil, err
	}
	return payload, nil
}
