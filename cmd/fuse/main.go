package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	pb "github.com/MiltonJ23/WouriFS/api/gen/v1/namenode"
	datanodepb "github.com/MiltonJ23/WouriFS/api/gen/v1/datanode"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// wourifsNode is the FUSE root inode backed by WouriFS gRPC calls.
type wourifsNode struct {
	fs.Inode
	path   string
	isDir  bool
	nnAddr string
	mu     sync.Mutex
}

var _ fs.NodeOnAdder = (*wourifsNode)(nil)

// onAdd is called when the inode is added to the tree — we pre-fetch attrs.
func (n *wourifsNode) OnAdd(ctx context.Context) {
	if n.path != "" {
		n.loadAttr(ctx)
	}
}

func (n *wourifsNode) loadAttr(ctx context.Context) {
	conn, err := dialNN(n.nnAddr)
	if err != nil {
		return
	}
	defer conn.Close()
	client := pb.NewNameNodeServiceClient(conn)
	resp, err := client.StatFile(ctx, &pb.StatFileRequest{Path: n.path})
	if err != nil {
		n.isDir = false
		return
	}
	n.isDir = resp.IsDir
}

// Lookup retrieves a child by name.
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
	client := pb.NewNameNodeServiceClient(conn)

	resp, err := client.StatFile(ctx, &pb.StatFileRequest{Path: childPath})
	if err != nil {
		return nil, syscall.ENOENT
	}

	out.Attr = fuse.Attr{
		Ino:   inoFromPath(childPath),
		Size:  uint64(resp.SizeBytes),
		Mode:  resp.Mode,
		Mtime: uint64(resp.MtimeUnix),
		Ctime: uint64(resp.CtimeUnix),
	}

	child := &wourifsNode{path: childPath, isDir: resp.IsDir, nnAddr: n.nnAddr}
	if resp.IsDir {
		out.Attr.Mode |= fuse.S_IFDIR
		return n.NewInode(ctx, child, fs.StableAttr{Mode: fuse.S_IFDIR, Ino: out.Attr.Ino}), 0
	}
	out.Attr.Mode |= fuse.S_IFREG
	return n.NewInode(ctx, child, fs.StableAttr{Mode: fuse.S_IFREG, Ino: out.Attr.Ino}), 0
}

// Getattr returns file/directory attributes.
func (n *wourifsNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	conn, err := dialNN(n.nnAddr)
	if err != nil {
		return syscall.EIO
	}
	defer conn.Close()
	client := pb.NewNameNodeServiceClient(conn)

	resp, err := client.StatFile(ctx, &pb.StatFileRequest{Path: n.path})
	if err != nil {
		// Root inode with empty path
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

	out.Attr = fuse.Attr{
		Ino:   inoFromPath(n.path),
		Size:  uint64(resp.SizeBytes),
		Mode:  mode,
		Mtime: uint64(resp.MtimeUnix),
		Ctime: uint64(resp.CtimeUnix),
	}
	return 0
}

// Create makes a new file.
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
	client := pb.NewNameNodeServiceClient(conn)

	_, err = client.CreateFile(ctx, &pb.CreateFileRequest{Path: childPath, Mode: mode})
	if err != nil {
		return nil, nil, 0, syscall.EIO
	}

	child := &wourifsNode{path: childPath, nnAddr: n.nnAddr}
	out.Attr.Ino = inoFromPath(childPath)
	out.Attr.Mode = fuse.S_IFREG | mode

	fh := &wourifsFileHandle{path: childPath, nnAddr: n.nnAddr, dnConns: make(map[string]datanodepb.DataNodeServiceClient)}
	return n.NewInode(ctx, child, fs.StableAttr{Mode: fuse.S_IFREG, Ino: out.Attr.Ino}), fh, 0, 0
}

// Mkdir creates a directory.
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
	client := pb.NewNameNodeServiceClient(conn)

	_, err = client.MakeDirectory(ctx, &pb.MakeDirectoryRequest{Path: childPath, Mode: mode})
	if err != nil {
		return nil, syscall.EIO
	}

	child := &wourifsNode{path: childPath, isDir: true, nnAddr: n.nnAddr}
	out.Attr.Ino = inoFromPath(childPath)
	out.Attr.Mode = fuse.S_IFDIR | mode
	return n.NewInode(ctx, child, fs.StableAttr{Mode: fuse.S_IFDIR, Ino: out.Attr.Ino}), 0
}

// Rmdir removes an empty directory.
func (n *wourifsNode) Rmdir(ctx context.Context, name string) syscall.Errno {
	childPath := n.path + "/" + name

	conn, err := dialNN(n.nnAddr)
	if err != nil {
		return syscall.EIO
	}
	defer conn.Close()
	client := pb.NewNameNodeServiceClient(conn)

	_, err = client.RemoveDirectory(ctx, &pb.RemoveDirectoryRequest{Path: childPath})
	if err != nil {
		return syscall.ENOTEMPTY
	}
	return 0
}

// Unlink removes a file.
func (n *wourifsNode) Unlink(ctx context.Context, name string) syscall.Errno {
	childPath := n.path + "/" + name

	conn, err := dialNN(n.nnAddr)
	if err != nil {
		return syscall.EIO
	}
	defer conn.Close()
	client := pb.NewNameNodeServiceClient(conn)

	_, err = client.DeleteFile(ctx, &pb.DeleteFileRequest{Path: childPath})
	if err != nil {
		return syscall.ENOENT
	}
	return 0
}

// Rename moves a file/directory.
func (n *wourifsNode) Rename(ctx context.Context, name string, newParent fs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	newDir := newParent.EmbeddedInode()
	newNode, ok := newDir.Operations().(*wourifsNode)
	if !ok {
		return syscall.EIO
	}

	oldPath := n.path + "/" + name
	if n.path == "" {
		oldPath = "/" + name
	}
	newPath := newNode.path + "/" + newName
	if newNode.path == "" {
		newPath = "/" + newName
	}

	conn, err := dialNN(n.nnAddr)
	if err != nil {
		return syscall.EIO
	}
	defer conn.Close()
	client := pb.NewNameNodeServiceClient(conn)

	_, err = client.RenameFile(ctx, &pb.RenameFileRequest{OldPath: oldPath, NewPath: newPath})
	if err != nil {
		return syscall.ENOENT
	}
	return 0
}

// Readdir lists directory contents.
func (n *wourifsNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	conn, err := dialNN(n.nnAddr)
	if err != nil {
		return nil, syscall.EIO
	}
	defer conn.Close()
	client := pb.NewNameNodeServiceClient(conn)

	resp, err := client.ListDirectory(ctx, &pb.ListDirectoryRequest{Path: n.path})
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

		entries = append(entries, fuse.DirEntry{
			Name: e.Name,
			Ino:  inoFromPath(childPath),
			Mode: mode,
		})
	}

	return fs.NewListDirStream(entries), 0
}

// --- File handle for read/write operations ---

type wourifsFileHandle struct {
	path    string
	nnAddr  string
	size    int64
	dnConns map[string]datanodepb.DataNodeServiceClient
	mu      sync.Mutex
}

var _ fs.FileReader = (*wourifsFileHandle)(nil)
var _ fs.FileWriter = (*wourifsFileHandle)(nil)
var _ fs.FileFlusher = (*wourifsFileHandle)(nil)

func (f *wourifsFileHandle) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	conn, err := dialNN(f.nnAddr)
	if err != nil {
		return nil, syscall.EIO
	}
	defer conn.Close()
	client := pb.NewNameNodeServiceClient(conn)

	lookup, err := client.LookupFile(ctx, &pb.LookupFileRequest{Path: f.path})
	if err != nil {
		return nil, syscall.ENOENT
	}

	if len(lookup.Chunks) == 0 {
		return fuse.ReadResultData([]byte{}), 0
	}

	// Find the right chunk for offset
	chunkSize := int64(64 << 20) // 64MB chunks
	chunkIdx := int(off / chunkSize)
	if chunkIdx >= len(lookup.Chunks) {
		return fuse.ReadResultData([]byte{}), 0
	}

	chunk := lookup.Chunks[chunkIdx]
	offsetInChunk := off % chunkSize

	dnConn, err := dialNN(chunk.DatanodeAddress)
	if err != nil {
		return nil, syscall.EIO
	}
	defer dnConn.Close()
	dnClient := datanodepb.NewDataNodeServiceClient(dnConn)

	stream, err := dnClient.ReadChunk(ctx, &datanodepb.ReadChunkRequest{ChunkId: chunk.ChunkId})
	if err != nil {
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

	if offsetInChunk >= int64(len(data)) {
		return fuse.ReadResultData([]byte{}), 0
	}

	end := offsetInChunk + int64(len(dest))
	if end > int64(len(data)) {
		end = int64(len(data))
	}
	return fuse.ReadResultData(data[offsetInChunk:end]), 0
}

func (f *wourifsFileHandle) Write(ctx context.Context, data []byte, off int64) (uint32, syscall.Errno) {
	conn, err := dialNN(f.nnAddr)
	if err != nil {
		return 0, syscall.EIO
	}
	defer conn.Close()
	client := pb.NewNameNodeServiceClient(conn)

	// Ensure chunk is allocated
	chunkSize := int64(64 << 20)
	chunkIdx := int32(off / chunkSize)

	allocResp, err := client.AllocateChunk(ctx, &pb.AllocateChunkRequest{
		FileId:            "", // will be resolved by path
		ChunkIndex:        chunkIdx,
		ReplicationFactor: 3,
	})
	if err != nil {
		return 0, syscall.EIO
	}

	// Write to first datanode
	dnAddr := allocResp.DatanodeAddresses[0]
	dnConn, err := dialNN(dnAddr)
	if err != nil {
		return 0, syscall.EIO
	}
	defer dnConn.Close()
	dnClient := datanodepb.NewDataNodeServiceClient(dnConn)

	wstream, err := dnClient.WriteChunk(ctx)
	if err != nil {
		return 0, syscall.EIO
	}

	wstream.Send(&datanodepb.WriteChunkRequest{
		ChunkId:    allocResp.ChunkId,
		Data:       data,
		BlockIndex: 0,
		IsLast:     true,
	})

	_, err = wstream.CloseAndRecv()
	if err != nil {
		return 0, syscall.EIO
	}

	return uint32(len(data)), 0
}

func (f *wourifsFileHandle) Flush(ctx context.Context) syscall.Errno {
	return 0
}

// --- Helpers ---

func dialNN(addr string) (*grpc.ClientConn, error) {
	return grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
}

func inoFromPath(path string) uint64 {
	var h uint64
	for _, c := range path {
		h = h*31 + uint64(c)
	}
	if h == 0 {
		h = 1
	}
	return h
}

// --- Main ---

func main() {
	mountPoint := flag.String("mount", "/tmp/wourifs", "FUSE mount point")
	nnAddr := flag.String("namenode", "127.0.0.1:9000", "Namenode gRPC address")
	flag.Parse()

	if err := os.MkdirAll(*mountPoint, 0755); err != nil {
		log.Fatalf("mkdir mount point: %v", err)
	}

	root := &wourifsNode{path: "", isDir: true, nnAddr: *nnAddr}

	server, err := fs.Mount(*mountPoint, root, &fs.Options{
		MountOptions: fuse.MountOptions{
			Debug:      false,
			Name:       "wourifs",
			FsName:     "wourifs",
			AllowOther: true,
		},
	})
	if err != nil {
		log.Fatalf("mount: %v", err)
	}

	fmt.Printf("WouriFS mounted at %s (namenode %s)\n", *mountPoint, *nnAddr)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		fmt.Println("\nunmounting...")
		server.Unmount()
	}()

	server.Wait()
	fmt.Println("unmounted")
}

// Suppress unused imports warning
var _ = time.Now
