package fuse

import (
	"context"
	"log"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
)

// directory read
var _ = fs.HandleReadDirAller(&Dir{})

func (d *Dir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	var ret []fuse.Dirent

	var currentInode = d.inode
	childInodes, err := backend.GetDirectoryContentsForInode(d.db, int32(currentInode))
	if err != nil {
		log.Println(err)
		return ret, fuse.ENOENT
	}

	for _, childInode := range childInodes {
		var dirent fuse.Dirent

		metadata, err := backend.GetMetadataForInode(d.db, childInode)
		if err == nil {
			dirent.Inode = uint64(childInode)
			dirent.Name = metadata.Name
			dirent.Type = fuse.DirentType(metadata.Type)

			ret = append(ret, dirent)
		} else {
			log.Println(err)
		}
	}

	return ret, nil
}

// directory lookup
var _ = fs.NodeRequestLookuper(&Dir{})

func (d *Dir) Lookup(ctx context.Context, req *fuse.LookupRequest, res *fuse.LookupResponse) (fs.Node, error) {
	path := req.Name

	dirents, err := d.ReadDirAll(ctx)
	if err != nil {
		log.Println("Couldn't ReadDirAll!")
		return nil, fuse.ENOENT
	}

	for _, dirent := range dirents {
		if dirent.Name == path {
			// yeah, looking up the db twice :(
			// TODO: make it faster. extract relevant stuff from ReadDirAll
			metadata, err := backend.GetMetadataForInode(d.db, int32(dirent.Inode))
			if err != nil {
				log.Println("Couldn't get metadata for inode!")
				return nil, fuse.ENOENT
			}

			switch metadata.Type {
			case int64(fuse.DT_File):
				return &File{d.db, int32(metadata.Inode)}, nil
			case int64(fuse.DT_Dir):
				return &Dir{d.db, int32(metadata.Inode)}, nil
			default:
				return nil, fuse.ENOENT
			}
		}
	}

	return nil, fuse.ENOENT
}

var _ = fs.NodeMkdirer(&Dir{})

func (d *Dir) Mkdir(ctx context.Context, req *fuse.MkdirRequest) (fs.Node, error) {
	inode, err := backend.CreateDirUnderInode(d.db, d.inode, req.Name)
	if err != nil {
		log.Println("Couldn't Mkdir!")
		return nil, err
	}

	return &Dir{d.db, inode}, nil
}

// TODO: impl NodeOpener for Dir?
