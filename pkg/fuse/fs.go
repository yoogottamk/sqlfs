package fuse

import (
	"context"
	"database/sql"
	"log"
	"os"
	"time"

	"bazil.org/fuse/fs"
	"github.com/yoogottamk/sqlfs/pkg/sqlutils"

	"bazil.org/fuse"
)

// filesystem
type FS struct {
	db *sql.DB
}

var _ fs.FS = (*FS)(nil)

func (f *FS) Root() (fs.Node, error) {
	metadata, err := Backend.GetMetadataForInode(f.db, 1)
	if err != nil {
		return &Dir{}, err
	}
	return &Dir{f.db, int32(metadata.Inode)}, nil
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

var _ fs.NodeSetattrer = (*File)(nil)
var _ fs.NodeSetattrer = (*File)(nil)

func handleSetattrRequest(db *sql.DB, inode int32, req *fuse.SetattrRequest) (sqlutils.Metadata, error) {
	metadata, err := Backend.GetMetadataForInode(db, inode)
	if err != nil {
		log.Println("Couldn't get metadata for setattr!")
		return metadata, err
	}

	var currentTimeNs = time.Now().UnixNano()

	if req.Valid.Atime() {
		// we store time in ns
		metadata.Atime = req.Atime.UnixMicro() * 1000
	}
	if req.Valid.AtimeNow() {
		metadata.Atime = currentTimeNs
	}

	if req.Valid.Gid() {
		metadata.Gid = int64(req.Gid)
	}

	if req.Valid.Mode() {
		if req.Mode&os.ModeIrregular > 0 {
			metadata.Mode = int64(req.Mode ^ os.ModeIrregular)
		}
		metadata.Mode = int64(req.Mode)
	}

	if req.Valid.Mtime() {
		// we store time in ns
		metadata.Mtime = req.Mtime.UnixMicro() * 1000
	}
	if req.Valid.MtimeNow() {
		metadata.Mtime = currentTimeNs
	}

	if req.Valid.Size() {
		metadata.Size = int64(req.Size)
	}

	if req.Valid.Uid() {
		metadata.Uid = int64(req.Uid)
	}

	return metadata, nil
}

func (f *File) Setattr(ctx context.Context, req *fuse.SetattrRequest, res *fuse.SetattrResponse) error {
	metadata, err := handleSetattrRequest(f.db, f.inode, req)
	if err != nil {
		return err
	}

	if err := Backend.SetMetadataForInode(f.db, f.inode, metadata); err != nil {
		log.Println("Failed to set metadata in setattr!")
		return err
	}

	return nil
}

func (d *Dir) Setattr(ctx context.Context, req *fuse.SetattrRequest, res *fuse.SetattrResponse) error {
	metadata, err := handleSetattrRequest(d.db, d.inode, req)
	if err != nil {
		return err
	}

	metadata.Mode ^= int64(os.ModeDir)

	if err := Backend.SetMetadataForInode(d.db, d.inode, metadata); err != nil {
		log.Println("Failed to set metadata in setattr!")
		return err
	}

	return nil
}
