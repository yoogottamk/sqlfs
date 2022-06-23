// Package fuse contains core fuse-related code. Uses SQLBackend interface
// to implement fs operations
package fuse

import (
	"bazil.org/fuse/fs"
	sql "github.com/jmoiron/sqlx"
)

// FS represents the file system itself
type FS struct {
	db *sql.DB
}

var _ fs.FS = (*FS)(nil)

// Root returns the root directory on fs
func (f *FS) Root() (fs.Node, error) {
	metadata, err := Backend.GetMetadataForInode(f.db, 1)
	if err != nil {
		return &Dir{}, err
	}
	return &Dir{f.db, int32(metadata.Inode)}, nil
}

// Dir represents a on fs
type Dir struct {
	db    *sql.DB
	inode int32
}

// File represents a file on fs
type File struct {
	db    *sql.DB
	inode int32
}
