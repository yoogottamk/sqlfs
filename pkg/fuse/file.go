package fuse

import (
	"context"
	"database/sql"
	"log"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
)

type FileHandle struct {
	db    *sql.DB
	inode int32
	file  *File
}

var _ fs.Handle = (*FileHandle)(nil)
var _ = fs.HandleReader(&FileHandle{})
var _ = fs.HandleWriter(&FileHandle{})

func (fh *FileHandle) Read(ctx context.Context, req *fuse.ReadRequest, res *fuse.ReadResponse) error {
	data, err := Backend.GetFileContentsForInode(fh.db, fh.inode)
	if err != nil {
		log.Println("Couldn't read file contents!")
		return err
	}

	res.Data = data

	return nil
}

func (fh *FileHandle) Write(ctx context.Context, req *fuse.WriteRequest, res *fuse.WriteResponse) error {
	data, err := Backend.GetFileContentsForInode(fh.db, fh.inode)
	if err != nil {
		log.Println("Couldn't read file contents!")
		return err
	}

	newData := append(data[:req.Offset], req.Data...)
	err = Backend.SetFileContentsForInode(fh.db, fh.inode, newData)
	if err != nil {
		log.Println("Failed to write to file!")
		return err
	}

	res.Size = len(req.Data)

	return nil
}

// open file (to get FileHandle)
var _ = fs.NodeOpener(&File{})

func (f *File) Open(ctx context.Context, req *fuse.OpenRequest, res *fuse.OpenResponse) (fs.Handle, error) {
	res.Flags |= fuse.OpenNonSeekable
	return &FileHandle{f.db, f.inode, f}, nil
}

// release file handle
var _ fs.HandleReleaser = (*FileHandle)(nil)

func (fh *FileHandle) Release(ctx context.Context, req *fuse.ReleaseRequest) error {
	// nothing to do
	return nil
}
