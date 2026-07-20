package main

import (
	"context"
	"io"
	"sync"
	"syscall"
	"time"

	pb "github.com/MiltonJ23/WouriFS/api/gen/v1/namenode"
	datanodepb "github.com/MiltonJ23/WouriFS/api/gen/v1/datanode"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const rpcTimeout = 10 * time.Second

// mapGRPCErr translates a gRPC error into a syscall errno for FUSE.
// If err is not a gRPC status error, fallback is returned.
func mapGRPCErr(err error, fallback syscall.Errno) syscall.Errno {
	if err == nil {
		return 0
	}
	st, ok := status.FromError(err)
	if !ok {
		return fallback
	}
	switch st.Code() {
	case codes.NotFound:
		return syscall.ENOENT
	case codes.PermissionDenied:
		return syscall.EACCES
	case codes.AlreadyExists:
		return syscall.EEXIST
	case codes.Unavailable:
		return syscall.EAGAIN
	default:
		return fallback
	}
}

// dialNN creates a gRPC connection. Callers should reuse the connection.
func dialNN(addr string) (*grpc.ClientConn, error) {
	return grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(64<<20)),
	)
}

// wourifsNode is the FUSE root inode backed by WouriFS gRPC.
// It holds a long-lived Namenode connection reused across operations.
type wourifsNode struct {
	fs.Inode
	path   string
	isDir  bool
	nnAddr string
	mu     sync.Mutex
}

// Compile-time interface checks.
var (
	_ fs.NodeOnAdder   = (*wourifsNode)(nil)
	_ fs.NodeLookuper  = (*wourifsNode)(nil)
	_ fs.NodeGetattrer = (*wourifsNode)(nil)
	_ fs.NodeSetattrer = (*wourifsNode)(nil)
	_ fs.NodeOpener    = (*wourifsNode)(nil)
	_ fs.NodeCreater   = (*wourifsNode)(nil)
	_ fs.NodeMkdirer   = (*wourifsNode)(nil)
	_ fs.NodeRmdirer   = (*wourifsNode)(nil)
	_ fs.NodeUnlinker  = (*wourifsNode)(nil)
	_ fs.NodeRenamer   = (*wourifsNode)(nil)
	_ fs.NodeReaddirer = (*wourifsNode)(nil)
)

func (n *wourifsNode) OnAdd(ctx context.Context) {
	if n.path == "" {
		return
	}
	conn, err := dialNN(n.nnAddr)
	if err != nil {
		return
	}
	defer conn.Close()
	rctx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()
	resp, err := pb.NewNameNodeServiceClient(conn).StatFile(rctx, &pb.StatFileRequest{Path: n.path})
	if err == nil {
		n.isDir = resp.IsDir
	}
}

func (n *wourifsNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	cp := childPath(n.path, name)
	conn, err := dialNN(n.nnAddr)
	if err != nil {
		return nil, syscall.EIO
	}
	defer conn.Close()
	rctx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()
	resp, err := pb.NewNameNodeServiceClient(conn).StatFile(rctx, &pb.StatFileRequest{Path: cp})
	if err != nil {
		return nil, mapGRPCErr(err, syscall.ENOENT)
	}
	out.Attr = fuse.Attr{
		Ino: ino(cp), Size: uint64(resp.SizeBytes), Mode: resp.Mode,
		Mtime: uint64(resp.MtimeUnix), Ctime: uint64(resp.CtimeUnix),
	}
	child := &wourifsNode{path: cp, isDir: resp.IsDir, nnAddr: n.nnAddr}
	if resp.IsDir {
		out.Attr.Mode |= fuse.S_IFDIR
		return n.NewInode(ctx, child, fs.StableAttr{Mode: fuse.S_IFDIR, Ino: out.Attr.Ino}), 0
	}
	out.Attr.Mode |= fuse.S_IFREG
	return n.NewInode(ctx, child, fs.StableAttr{Mode: fuse.S_IFREG, Ino: out.Attr.Ino}), 0
}

func (n *wourifsNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	conn, err := dialNN(n.nnAddr)
	if err != nil {
		return syscall.EIO
	}
	defer conn.Close()
	rctx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()
	resp, err := pb.NewNameNodeServiceClient(conn).StatFile(rctx, &pb.StatFileRequest{Path: n.path})
	if err != nil {
		return mapGRPCErr(err, syscall.EIO)
	}
	mode := resp.Mode
	if mode == 0 {
		if resp.IsDir {
			mode = 0755
		} else {
			mode = 0644
		}
	}
	if resp.IsDir {
		mode |= fuse.S_IFDIR
	} else {
		mode |= fuse.S_IFREG
	}
	out.Attr = fuse.Attr{
		Ino: ino(n.path), Size: uint64(resp.SizeBytes), Mode: mode,
		Mtime: uint64(resp.MtimeUnix), Ctime: uint64(resp.CtimeUnix),
	}
	return 0
}

func (n *wourifsNode) Setattr(ctx context.Context, fh fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	conn, err := dialNN(n.nnAddr)
	if err != nil {
		return syscall.EIO
	}
	defer conn.Close()
	client := pb.NewNameNodeServiceClient(conn)
	if in.Valid&fuse.FATTR_SIZE != 0 {
		if sz, ok := in.GetSize(); ok {
			rctx, cancel := context.WithTimeout(ctx, rpcTimeout)
			defer cancel()
			if _, err := client.TruncateFile(rctx, &pb.TruncateFileRequest{Path: n.path, SizeBytes: int64(sz)}); err != nil {
				return mapGRPCErr(err, syscall.EIO)
			}
		}
	}
	return n.Getattr(ctx, fh, out)
}

func (n *wourifsNode) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	conn, err := dialNN(n.nnAddr)
	if err != nil {
		return nil, 0, syscall.EIO
	}
	defer conn.Close()
	rctx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()
	resp, err := pb.NewNameNodeServiceClient(conn).LookupFile(rctx, &pb.LookupFileRequest{Path: n.path})
	if err != nil {
		return nil, 0, mapGRPCErr(err, syscall.ENOENT)
	}
	fh := &wourifsFileHandle{
		path:    n.path,
		fileID:  resp.FileId,
		nnAddr:  n.nnAddr,
		dnConns: make(map[string]*grpc.ClientConn),
	}
	return fh, fuse.FOPEN_KEEP_CACHE, 0
}

func (n *wourifsNode) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (*fs.Inode, fs.FileHandle, uint32, syscall.Errno) {
	cp := childPath(n.path, name)
	conn, err := dialNN(n.nnAddr)
	if err != nil {
		return nil, nil, 0, syscall.EIO
	}
	defer conn.Close()
	rctx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()
	resp, err := pb.NewNameNodeServiceClient(conn).CreateFile(rctx, &pb.CreateFileRequest{Path: cp, Mode: mode})
	if err != nil {
		return nil, nil, 0, mapGRPCErr(err, syscall.EIO)
	}
	child := &wourifsNode{path: cp, nnAddr: n.nnAddr}
	out.Attr.Ino = ino(cp)
	out.Attr.Mode = fuse.S_IFREG | mode
	fh := &wourifsFileHandle{
		path:    cp,
		fileID:  resp.FileId,
		nnAddr:  n.nnAddr,
		dnConns: make(map[string]*grpc.ClientConn),
	}
	return n.NewInode(ctx, child, fs.StableAttr{Mode: fuse.S_IFREG, Ino: out.Attr.Ino}), fh, 0, 0
}

func (n *wourifsNode) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	cp := childPath(n.path, name)
	conn, err := dialNN(n.nnAddr)
	if err != nil {
		return nil, syscall.EIO
	}
	defer conn.Close()
	rctx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()
	if _, err := pb.NewNameNodeServiceClient(conn).MakeDirectory(rctx, &pb.MakeDirectoryRequest{Path: cp, Mode: mode}); err != nil {
		return nil, mapGRPCErr(err, syscall.EIO)
	}
	child := &wourifsNode{path: cp, isDir: true, nnAddr: n.nnAddr}
	out.Attr.Ino = ino(cp)
	out.Attr.Mode = fuse.S_IFDIR | mode
	return n.NewInode(ctx, child, fs.StableAttr{Mode: fuse.S_IFDIR, Ino: out.Attr.Ino}), 0
}

// childPath constructs a child path, handling the root case.
func childPath(parent, name string) string {
	if parent == "" || parent == "/" {
		return "/" + name
	}
	return parent + "/" + name
}

func (n *wourifsNode) Rmdir(ctx context.Context, name string) syscall.Errno {
	conn, err := dialNN(n.nnAddr)
	if err != nil {
		return syscall.EIO
	}
	defer conn.Close()
	rctx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()
	_, err = pb.NewNameNodeServiceClient(conn).RemoveDirectory(rctx, &pb.RemoveDirectoryRequest{Path: childPath(n.path, name)})
	return mapGRPCErr(err, syscall.ENOTEMPTY)
}

func (n *wourifsNode) Unlink(ctx context.Context, name string) syscall.Errno {
	conn, err := dialNN(n.nnAddr)
	if err != nil {
		return syscall.EIO
	}
	defer conn.Close()
	rctx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()
	_, err = pb.NewNameNodeServiceClient(conn).DeleteFile(rctx, &pb.DeleteFileRequest{Path: childPath(n.path, name)})
	return mapGRPCErr(err, syscall.ENOENT)
}

func (n *wourifsNode) Rename(ctx context.Context, name string, newParent fs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	newDir := newParent.EmbeddedInode()
	newNode, ok := newDir.Operations().(*wourifsNode)
	if !ok {
		return syscall.EIO
	}
	oldPath := childPath(n.path, name)
	newPath := childPath(newNode.path, newName)
	conn, err := dialNN(n.nnAddr)
	if err != nil {
		return syscall.EIO
	}
	defer conn.Close()
	rctx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()
	_, err = pb.NewNameNodeServiceClient(conn).RenameFile(rctx, &pb.RenameFileRequest{OldPath: oldPath, NewPath: newPath})
	return mapGRPCErr(err, syscall.ENOENT)
}

func (n *wourifsNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	conn, err := dialNN(n.nnAddr)
	if err != nil {
		return nil, syscall.EIO
	}
	defer conn.Close()
	rctx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()
	resp, err := pb.NewNameNodeServiceClient(conn).ListDirectory(rctx, &pb.ListDirectoryRequest{Path: n.path})
	if err != nil {
		return nil, mapGRPCErr(err, syscall.EIO)
	}
	var entries []fuse.DirEntry
	for _, e := range resp.Entries {
		mode := e.Mode
		if mode == 0 {
			if e.IsDir {
				mode = 0755
			} else {
				mode = 0644
			}
		}
		if e.IsDir {
			mode |= fuse.S_IFDIR
		} else {
			mode |= fuse.S_IFREG
		}
		childPath := childPath(n.path, e.Name)
		entries = append(entries, fuse.DirEntry{Name: e.Name, Ino: ino(childPath), Mode: mode})
	}
	return fs.NewListDirStream(entries), 0
}

// --- File handle ---

type wourifsFileHandle struct {
	path    string
	fileID  string
	nnAddr  string
	dnConns map[string]*grpc.ClientConn
	mu      sync.Mutex
}

var (
	_ fs.FileReader   = (*wourifsFileHandle)(nil)
	_ fs.FileWriter   = (*wourifsFileHandle)(nil)
	_ fs.FileFsyncer  = (*wourifsFileHandle)(nil)
	_ fs.FileFlusher  = (*wourifsFileHandle)(nil)
	_ fs.FileReleaser = (*wourifsFileHandle)(nil)
)

// getDNConn returns a cached Datanode connection or creates one.
func (f *wourifsFileHandle) getDNConn(addr string) (*grpc.ClientConn, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if conn, ok := f.dnConns[addr]; ok {
		return conn, nil
	}
	conn, err := dialNN(addr)
	if err != nil {
		return nil, err
	}
	f.dnConns[addr] = conn
	return conn, nil
}

func (f *wourifsFileHandle) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	conn, err := dialNN(f.nnAddr)
	if err != nil {
		return nil, syscall.EIO
	}
	defer conn.Close()
	rctx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()
	lookup, err := pb.NewNameNodeServiceClient(conn).LookupFile(rctx, &pb.LookupFileRequest{Path: f.path})
	if err != nil {
		return nil, mapGRPCErr(err, syscall.ENOENT)
	}
	if len(lookup.Chunks) == 0 {
		return fuse.ReadResultData([]byte{}), 0
	}
	chunkSize := int64(64 << 20)
	chunkIdx := int(off / chunkSize)
	if chunkIdx >= len(lookup.Chunks) {
		return fuse.ReadResultData([]byte{}), 0
	}
	chunk := lookup.Chunks[chunkIdx]
	offChunk := off % chunkSize

	dnConn, err := f.getDNConn(chunk.DatanodeAddress)
	if err != nil {
		return nil, syscall.EIO
	}
	rctx2, cancel2 := context.WithTimeout(ctx, rpcTimeout)
	defer cancel2()
	stream, err := datanodepb.NewDataNodeServiceClient(dnConn).ReadChunk(rctx2, &datanodepb.ReadChunkRequest{ChunkId: chunk.ChunkId})
	if err != nil {
		return nil, mapGRPCErr(err, syscall.EIO)
	}
	var data []byte
	for {
		resp, ioErr := stream.Recv()
		if ioErr == io.EOF {
			break
		}
		if ioErr != nil {
			return nil, mapGRPCErr(ioErr, syscall.EIO)
		}
		data = append(data, resp.Data...)
		if resp.IsLast {
			break
		}
	}
	if offChunk >= int64(len(data)) {
		return fuse.ReadResultData([]byte{}), 0
	}
	end := offChunk + int64(len(dest))
	if end > int64(len(data)) {
		end = int64(len(data))
	}
	return fuse.ReadResultData(data[offChunk:end]), 0
}

func (f *wourifsFileHandle) Write(ctx context.Context, data []byte, off int64) (uint32, syscall.Errno) {
	conn, err := dialNN(f.nnAddr)
	if err != nil {
		return 0, syscall.EIO
	}
	defer conn.Close()
	rctx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()
	client := pb.NewNameNodeServiceClient(conn)
	chunkIdx := int32(off / (64 << 20))
	alloc, err := client.AllocateChunk(rctx, &pb.AllocateChunkRequest{FileId: f.fileID, ChunkIndex: chunkIdx, ReplicationFactor: 3})
	if err != nil {
		return 0, mapGRPCErr(err, syscall.EIO)
	}
	if len(alloc.DatanodeAddresses) == 0 {
		return 0, syscall.EIO
	}
	dnConn, err := f.getDNConn(alloc.DatanodeAddresses[0])
	if err != nil {
		return 0, syscall.EIO
	}
	rctx2, cancel2 := context.WithTimeout(ctx, rpcTimeout)
	defer cancel2()
	wstream, err := datanodepb.NewDataNodeServiceClient(dnConn).WriteChunk(rctx2)
	if err != nil {
		return 0, mapGRPCErr(err, syscall.EIO)
	}
	wstream.Send(&datanodepb.WriteChunkRequest{ChunkId: alloc.ChunkId, Data: data, BlockIndex: 0, IsLast: true})
	if _, err = wstream.CloseAndRecv(); err != nil {
		return 0, mapGRPCErr(err, syscall.EIO)
	}
	return uint32(len(data)), 0
}

func (f *wourifsFileHandle) Fsync(ctx context.Context, flags uint32) syscall.Errno {
	conn, err := dialNN(f.nnAddr)
	if err != nil {
		return syscall.EIO
	}
	defer conn.Close()
	rctx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()
	_, err = pb.NewNameNodeServiceClient(conn).StatFile(rctx, &pb.StatFileRequest{Path: f.path})
	return mapGRPCErr(err, syscall.EIO)
}

func (f *wourifsFileHandle) Flush(ctx context.Context) syscall.Errno { return 0 }

func (f *wourifsFileHandle) Release(ctx context.Context) syscall.Errno {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, c := range f.dnConns {
		c.Close()
	}
	f.dnConns = nil
	return 0
}

// --- shared helpers ---

func ino(path string) uint64 {
	var h uint64
	for _, c := range path {
		h = h*31 + uint64(c)
	}
	if h == 0 {
		h = 1
	}
	return h
}

// keep imports alive
