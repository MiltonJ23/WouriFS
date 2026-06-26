package datanode

import (
	"bytes"
	"context"
	"io"
	"log"

	datanodepb "github.com/MiltonJ23/WouriFS/api/gen/v1/datanode"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Server implements datanodepb.DataNodeServiceServer (FR-D-002).
type Server struct {
	datanodepb.UnimplementedDataNodeServiceServer
	store *ChunkStore
}

// NewServer creates a DataNode gRPC handler.
func NewServer(store *ChunkStore) *Server {
	return &Server{store: store}
}

// Status returns storage capacity and health info for observability.
func (s *Server) Status(ctx context.Context, req *datanodepb.StatusRequest) (*datanodepb.StatusResponse, error) {
	total := s.store.TotalSize()
	return &datanodepb.StatusResponse{
		TotalBytes: 1 << 30, // configurable in production
		UsedBytes:  total,
		FreeBytes:  (1 << 30) - total,
		ChunkCount: int32(s.store.ChunkCount()),
	}, nil
}

// WriteChunk receives a client-side stream of 1MB blocks and persists the chunk (FR-D-002, FR-D-006).
func (s *Server) WriteChunk(stream datanodepb.DataNodeService_WriteChunkServer) error {
	var buf bytes.Buffer
	var chunkID string

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return status.Errorf(codes.Internal, "recv block: %v", err)
		}
		if chunkID == "" {
			chunkID = req.ChunkId
		}
		buf.Write(req.Data)
		if req.IsLast {
			break
		}
	}

	n, err := s.store.Write(chunkID, &buf)
	if err != nil {
		return status.Errorf(codes.Internal, "persist chunk: %v", err)
	}
	log.Printf("chunk written: %s (%d bytes)", chunkID, n)

	return stream.SendAndClose(&datanodepb.WriteChunkResponse{
		ChunkId:      chunkID,
		BytesWritten: n,
	})
}

// ReadChunk sends the chunk content via server-side streaming in 1MB blocks (FR-D-002, FR-D-006).
func (s *Server) ReadChunk(req *datanodepb.ReadChunkRequest, stream datanodepb.DataNodeService_ReadChunkServer) error {
	reader, size, err := s.store.Read(req.ChunkId)
	if err != nil {
		return status.Error(codes.NotFound, err.Error())
	}
	defer reader.Close()

	const blockSize = 1 << 20 // 1 MB
	buf := make([]byte, blockSize)
	index := int32(0)
	remaining := size

	for remaining > 0 {
		n, err := reader.Read(buf)
		if n > 0 {
			isLast := int64(n) >= remaining
			if sendErr := stream.Send(&datanodepb.ReadChunkResponse{
				Data:       buf[:n],
				BlockIndex: index,
				IsLast:     isLast,
			}); sendErr != nil {
				return sendErr
			}
			remaining -= int64(n)
			index++
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return status.Errorf(codes.Internal, "read chunk: %v", err)
		}
	}

	return nil
}

// DeleteChunk removes a chunk from local storage (FR-D-002).
func (s *Server) DeleteChunk(ctx context.Context, req *datanodepb.DeleteChunkRequest) (*datanodepb.DeleteChunkResponse, error) {
	if err := s.store.Delete(req.ChunkId); err != nil {
		return nil, status.Errorf(codes.NotFound, "delete chunk: %v", err)
	}
	log.Printf("chunk deleted: %s", req.ChunkId)
	return &datanodepb.DeleteChunkResponse{}, nil
}

// ReplicateChunk accepts a replicated chunk from a peer Datanode (FR-D-004).
func (s *Server) ReplicateChunk(stream datanodepb.DataNodeService_ReplicateChunkServer) error {
	var buf bytes.Buffer
	var chunkID string

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return status.Errorf(codes.Internal, "recv replicate block: %v", err)
		}
		if chunkID == "" {
			chunkID = req.ChunkId
		}
		buf.Write(req.Data)
		if req.IsLast {
			break
		}
	}

	// Skip if already present (idempotent replication)
	if s.store.Exists(chunkID) {
		return stream.SendAndClose(&datanodepb.ReplicateChunkResponse{
			ChunkId:      chunkID,
			BytesWritten: 0,
		})
	}

	n, err := s.store.Write(chunkID, &buf)
	if err != nil {
		return status.Errorf(codes.Internal, "replicate chunk: %v", err)
	}
	log.Printf("chunk replicated: %s (%d bytes)", chunkID, n)

	return stream.SendAndClose(&datanodepb.ReplicateChunkResponse{
		ChunkId:      chunkID,
		BytesWritten: n,
	})
}
