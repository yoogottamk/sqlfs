package fuse

import (
	"database/sql"

	"bazil.org/fuse/fs"
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
