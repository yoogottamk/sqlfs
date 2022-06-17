package fuse

import (
	"bazil.org/fuse/fs"
	"context"
	"database/sql"
	"log"
	"os"
	"time"

	"bazil.org/fuse"
)

// filesystem
type FS struct {
	Db *sql.DB
}

var _ fs.FS = (*FS)(nil)

func (f *FS) Root() (fs.Node, error) {
	metadata, err := Backend.GetMetadataForInode(f.Db, 1)
	if err != nil {
		return &Dir{}, err
	}
	return &Dir{f.Db, int32(metadata.Inode)}, nil
}

// directories, files
type Dir struct {
	db    *sql.DB
	inode int32
}
type File struct {
	db    *sql.DB
	inode int32
}

var _ fs.Node = (*Dir)(nil)
var _ fs.Node = (*File)(nil)

func setAttrFromMetadata(db *sql.DB, inode int32, attr *fuse.Attr) error {
	metadata, err := Backend.GetMetadataForInode(db, inode)
	if err != nil {
		log.Println("Failed to update metadata for dir!")
		return err
	}

	attr.Uid = uint32(metadata.Uid)
	attr.Gid = uint32(metadata.Gid)
	attr.Mode = os.FileMode(metadata.Mode)
	attr.Ctime = time.Unix(metadata.Ctime/1e9, metadata.Ctime%1e9)
	attr.Mtime = time.Unix(metadata.Mtime/1e9, metadata.Mtime%1e9)
	attr.Atime = time.Unix(metadata.Atime/1e9, metadata.Atime%1e9)
	attr.Size = uint64(metadata.Size)

	return nil
}

func (d *Dir) Attr(ctx context.Context, attr *fuse.Attr) (err error) {
	err = setAttrFromMetadata(d.db, d.inode, attr)
	return
}

func (f *File) Attr(ctx context.Context, attr *fuse.Attr) (err error) {
	err = setAttrFromMetadata(f.db, f.inode, attr)
	return
}
