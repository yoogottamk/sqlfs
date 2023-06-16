package fuse

import (
	"context"
	"log"
	"os"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	sql "github.com/jmoiron/sqlx"

	"github.com/yoogottamk/sqlfs/pkg/sqlutils"
)

var _ fs.Node = (*Dir)(nil)
var _ fs.Node = (*File)(nil)

// setAttrFromMetadata populates the fuse attr object with details fetched from
// db for the given inode
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

// Attr retrieves metadata attr for dir
func (d *Dir) Attr(ctx context.Context, attr *fuse.Attr) (err error) {
	err = setAttrFromMetadata(d.db, d.inode, attr)
	return
}

// Attr retrieves metadata attr for file
func (f *File) Attr(ctx context.Context, attr *fuse.Attr) (err error) {
	err = setAttrFromMetadata(f.db, f.inode, attr)
	return
}

var _ fs.NodeSetattrer = (*File)(nil)
var _ fs.NodeSetattrer = (*File)(nil)

// getUpdatedMetadataForSetattr generates the Metadata struct based on attributes
// from setattr req for a given inode. It loads the current values from db
// and returns the updated Metadata
func getUpdatedMetadataForSetattr(db *sql.DB, inode int32, req *fuse.SetattrRequest) (sqlutils.Metadata, error) {
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
		// update the file contents if new size is smaller
		// if truncate to 0 then N happens, the file contents should be null
		// if we dont update the contents now, it will show the old contents instead
		if req.Size < uint64(metadata.Size) {
			filedata, err := Backend.GetFileContentsForInode(db, inode)
			if err != nil {
				log.Printf("Couldn't retrieve file contents: %v\n", err)
				return metadata, err
			}
			err = Backend.SetFileContentsForInode(db, inode, filedata[:req.Size])
			if err != nil {
				log.Printf("Couldn't set file contents: %v\n", err)
			}
		}

		metadata.Size = int64(req.Size)
	}

	if req.Valid.Uid() {
		metadata.Uid = int64(req.Uid)
	}

	return metadata, nil
}

// Setattr updates the metadata table on db based on req
func (d *Dir) Setattr(ctx context.Context, req *fuse.SetattrRequest, res *fuse.SetattrResponse) error {
	metadata, err := getUpdatedMetadataForSetattr(d.db, d.inode, req)
	if err != nil {
		return err
	}

	if err := Backend.SetMetadataForInode(d.db, d.inode, metadata); err != nil {
		log.Println("Failed to set metadata in setattr!")
		return err
	}

	return nil
}

// Setattr updates the metadata table on db based on req
func (f *File) Setattr(ctx context.Context, req *fuse.SetattrRequest, res *fuse.SetattrResponse) error {
	metadata, err := getUpdatedMetadataForSetattr(f.db, f.inode, req)
	if err != nil {
		return err
	}

	if err := Backend.SetMetadataForInode(f.db, f.inode, metadata); err != nil {
		log.Println("Failed to set metadata in setattr!")
		return err
	}

	return nil
}
