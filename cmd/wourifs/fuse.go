package main

import (
	"context"
	"sync"
	"syscall"

	pb "github.com/MiltonJ23/WouriFS/api/gen/v1/namenode"
	datanodepb "github.com/MiltonJ23/WouriFS/api/gen/v1/datanode"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// --- FUSE inode ---

type wourifsNode struct {
	fs.Inode
	path   string
	isDir  bool
	nnAddr string
	mu     sync.Mutex
}

var _ fs.NodeOnAdder = (*wourifsNode)(nil)

func (n *wourifsNode) OnAdd(ctx context.Context) {
	if n.path == "" {
		return
	}
	conn, _ := dialNN(n.nnAddr)
	if conn == nil {
		return
	}
	defer conn.Close()
	resp, err := pb.NewNameNodeServiceClient(conn).StatFile(ctx, &pb.StatFileRequest{Path: n.path})
	if err == nil {
		n.isDir = resp.IsDir
	}
}

func (n *wourifsNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	childPath := n.path + "/" + name
	if n.path == "" || n.path == "/" {
		childPath = "/" + name
	}
	conn, err := dialNN(n.nnAddr)
	if err != nil {
		return nil, syscall.EIO
	}
	defer conn.Close()
	resp, err := pb.NewNameNodeServiceClient(conn).StatFile(ctx, &pb.StatFileRequest{Path: childPath})
	if err != nil {
		return nil, syscall.ENOENT
	}
	out.Attr = fuse.Attr{Ino: ino(childPath), Size: uint64(resp.SizeBytes), Mode: resp.Mode, Mtime: uint64(resp.MtimeUnix), Ctime: uint64(resp.CtimeUnix)}
	child := &wourifsNode{path: childPath, isDir: resp.IsDir, nnAddr: n.nnAddr}
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
	resp, err := pb.NewNameNodeServiceClient(conn).StatFile(ctx, &pb.StatFileRequest{Path: n.path})
	if err != nil {
		out.Attr.Mode = fuse.S_IFDIR | 0755
		out.Attr.Ino = 1
		return 0
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
	out.Attr = fuse.Attr{Ino: ino(n.path), Size: uint64(resp.SizeBytes), Mode: mode, Mtime: uint64(resp.MtimeUnix), Ctime: uint64(resp.CtimeUnix)}
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
			client.TruncateFile(ctx, &pb.TruncateFileRequest{Path: n.path, SizeBytes: int64(sz)})
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
	resp, err := pb.NewNameNodeServiceClient(conn).LookupFile(ctx, &pb.LookupFileRequest{Path: n.path})
	if err != nil {
		return nil, 0, syscall.ENOENT
	}
	return &wourifsFileHandle{path: n.path, fileID: resp.FileId, nnAddr: n.nnAddr, dnConns: map[string]datanodepb.DataNodeServiceClient{}}, fuse.FOPEN_KEEP_CACHE, 0
}

func (n *wourifsNode) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (*fs.Inode, fs.FileHandle, uint32, syscall.Errno) {
	childPath := n.path + "/" + name
	if n.path == "" {
		childPath = "/" + name
	}
	conn, err := dialNN(n.nnAddr)
	if err != nil {
		return nil, nil, 0, syscall.EIO
	}
	defer conn.Close()
	resp, err := pb.NewNameNodeServiceClient(conn).CreateFile(ctx, &pb.CreateFileRequest{Path: childPath, Mode: mode})
	if err != nil {
		return nil, nil, 0, syscall.EIO
	}
	child := &wourifsNode{path: childPath, nnAddr: n.nnAddr}
	out.Attr.Ino = ino(childPath)
	out.Attr.Mode = fuse.S_IFREG | mode
	fh := &wourifsFileHandle{path: childPath, fileID: resp.FileId, nnAddr: n.nnAddr, dnConns: map[string]datanodepb.DataNodeServiceClient{}}
	return n.NewInode(ctx, child, fs.StableAttr{Mode: fuse.S_IFREG, Ino: out.Attr.Ino}), fh, 0, 0
}

func (n *wourifsNode) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	childPath := n.path + "/" + name
	if n.path == "" {
		childPath = "/" + name
	}
	conn, err := dialNN(n.nnAddr)
	if err != nil {
		return nil, syscall.EIO
	}
	defer conn.Close()
	_, err = pb.NewNameNodeServiceClient(conn).MakeDirectory(ctx, &pb.MakeDirectoryRequest{Path: childPath, Mode: mode})
	if err != nil {
		return nil, syscall.EIO
	}
	child := &wourifsNode{path: childPath, isDir: true, nnAddr: n.nnAddr}
	out.Attr.Ino = ino(childPath)
	out.Attr.Mode = fuse.S_IFDIR | mode
	return n.NewInode(ctx, child, fs.StableAttr{Mode: fuse.S_IFDIR, Ino: out.Attr.Ino}), 0
}

func (n *wourifsNode) Rmdir(ctx context.Context, name string) syscall.Errno {
	conn, _ := dialNN(n.nnAddr)
	if conn == nil {
		return syscall.EIO
	}
	defer conn.Close()
	_, err := pb.NewNameNodeServiceClient(conn).RemoveDirectory(ctx, &pb.RemoveDirectoryRequest{Path: n.path + "/" + name})
	if err != nil {
		return syscall.ENOTEMPTY
	}
	return 0
}

func (n *wourifsNode) Unlink(ctx context.Context, name string) syscall.Errno {
	conn, _ := dialNN(n.nnAddr)
	if conn == nil {
		return syscall.EIO
	}
	defer conn.Close()
	_, err := pb.NewNameNodeServiceClient(conn).DeleteFile(ctx, &pb.DeleteFileRequest{Path: n.path + "/" + name})
	if err != nil {
		return syscall.ENOENT
	}
	return 0
}

func (n *wourifsNode) Rename(ctx context.Context, name string, newParent fs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	newDir := newParent.EmbeddedInode()
	newNode, ok := newDir.Operations().(*wourifsNode)
	if !ok {
		return syscall.EIO
	}
	oldPath := n.path + "/" + name
	newPath := newNode.path + "/" + newName
	if n.path == "" {
		oldPath = "/" + name
	}
	if newNode.path == "" {
		newPath = "/" + newName
	}
	conn, _ := dialNN(n.nnAddr)
	if conn == nil {
		return syscall.EIO
	}
	defer conn.Close()
	_, err := pb.NewNameNodeServiceClient(conn).RenameFile(ctx, &pb.RenameFileRequest{OldPath: oldPath, NewPath: newPath})
	if err != nil {
		return syscall.ENOENT
	}
	return 0
}

func (n *wourifsNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	conn, _ := dialNN(n.nnAddr)
	if conn == nil {
		return nil, syscall.EIO
	}
	defer conn.Close()
	resp, err := pb.NewNameNodeServiceClient(conn).ListDirectory(ctx, &pb.ListDirectoryRequest{Path: n.path})
	if err != nil {
		return nil, syscall.EIO
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
		childPath := n.path + "/" + e.Name
		if n.path == "" || n.path == "/" {
			childPath = "/" + e.Name
		}
		entries = append(entries, fuse.DirEntry{Name: e.Name, Ino: ino(childPath), Mode: mode})
	}
	return fs.NewListDirStream(entries), 0
}

// --- File handle ---

type wourifsFileHandle struct {
	path    string
	fileID  string
	nnAddr  string
	dnConns map[string]datanodepb.DataNodeServiceClient
	mu      sync.Mutex
}

var _ fs.FileReader = (*wourifsFileHandle)(nil)
var _ fs.FileWriter = (*wourifsFileHandle)(nil)
var _ fs.FileFsyncer = (*wourifsFileHandle)(nil)
var _ fs.FileFlusher = (*wourifsFileHandle)(nil)

func (f *wourifsFileHandle) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	conn, _ := dialNN(f.nnAddr)
	if conn == nil {
		return nil, syscall.EIO
	}
	defer conn.Close()
	lookup, err := pb.NewNameNodeServiceClient(conn).LookupFile(ctx, &pb.LookupFileRequest{Path: f.path})
	if err != nil {
		return nil, syscall.ENOENT
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
	dnConn, _ := dialNN(chunk.DatanodeAddress)
	if dnConn == nil {
		return nil, syscall.EIO
	}
	defer dnConn.Close()
	stream, _ := datanodepb.NewDataNodeServiceClient(dnConn).ReadChunk(ctx, &datanodepb.ReadChunkRequest{ChunkId: chunk.ChunkId})
	if stream == nil {
		return nil, syscall.EIO
	}
	var data []byte
	for {
		resp, err := stream.Recv()
		if err != nil {
			break
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
	conn, _ := dialNN(f.nnAddr)
	if conn == nil {
		return 0, syscall.EIO
	}
	defer conn.Close()
	client := pb.NewNameNodeServiceClient(conn)
	chunkIdx := int32(off / (64 << 20))
	alloc, err := client.AllocateChunk(ctx, &pb.AllocateChunkRequest{FileId: f.fileID, ChunkIndex: chunkIdx, ReplicationFactor: 3})
	if err != nil {
		return 0, syscall.EIO
	}
	if len(alloc.DatanodeAddresses) == 0 {
		return 0, syscall.EIO
	}
	dnConn, _ := dialNN(alloc.DatanodeAddresses[0])
	if dnConn == nil {
		return 0, syscall.EIO
	}
	defer dnConn.Close()
	wstream, _ := datanodepb.NewDataNodeServiceClient(dnConn).WriteChunk(ctx)
	if wstream == nil {
		return 0, syscall.EIO
	}
	wstream.Send(&datanodepb.WriteChunkRequest{ChunkId: alloc.ChunkId, Data: data, BlockIndex: 0, IsLast: true})
	_, err = wstream.CloseAndRecv()
	if err != nil {
		return 0, syscall.EIO
	}
	return uint32(len(data)), 0
}

func (f *wourifsFileHandle) Fsync(ctx context.Context, flags uint32) syscall.Errno {
	conn, _ := dialNN(f.nnAddr)
	if conn == nil {
		return syscall.EIO
	}
	defer conn.Close()
	_, err := pb.NewNameNodeServiceClient(conn).StatFile(ctx, &pb.StatFileRequest{Path: f.path})
	if err != nil {
		return syscall.EIO
	}
	return 0
}

func (f *wourifsFileHandle) Flush(ctx context.Context) syscall.Errno { return 0 }

func (f *wourifsFileHandle) Release(ctx context.Context) syscall.Errno {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, c := range f.dnConns {
		c.(interface{ Close() error }).Close()
	}
	f.dnConns = nil
	return 0
}

// --- helpers ---

func dialNN(addr string) (*grpc.ClientConn, error) {
	return grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
}

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
