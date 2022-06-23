package sqlutils

import (
	"errors"
	"log"
	"os"
	"syscall"
	"time"

	"bazil.org/fuse"
	sql "github.com/jmoiron/sqlx"
)

var AvaialableBackends = map[string]SQLBackend{
	"sqlite":   SQLiteBackend{},
	"mysql":    MySQLBackend{},
	"postgres": PostgresBackend{},
}

// Metadata table as go struct
type Metadata struct {
	Inode int64 `db:"inode"`

	Uid int64 `db:"uid"`
	Gid int64 `db:"gid"`

	Mode int64 `db:"mode"`
	Type int64 `db:"type"`

	Ctime int64 `db:"ctime"`
	Atime int64 `db:"atime"`
	Mtime int64 `db:"mtime"`

	Name string `db:"name"`
	Size int64  `db:"size"`
}

type SQLBackend interface {
	OpenDB(dsn string) (*sql.DB, error)
	VerifyDB(db *sql.DB) error

	CreateDBTables(db *sql.DB) error
	InitializeDBRows(db *sql.DB) error

	GetMetadataForInode(db *sql.DB, inode int32) (Metadata, error)
	SetMetadataForInode(db *sql.DB, inode int32, metadata Metadata) error

	GetDirectoryContentsForInode(db *sql.DB, inode int32) ([]int32, error)

	GetFileContentsForInode(db *sql.DB, inode int32) ([]byte, error)
	SetFileContentsForInode(db *sql.DB, inode int32, data []byte) error

	// could've been the same function with an if condition
	// but maybe some backends might want to utilize the segregation
	CreateDirUnderInode(db *sql.DB, inode int32, name string) (int32, error)
	CreateFileUnderInode(db *sql.DB, inode int32, name string) (int32, error)

	// could've been the same function with an if condition
	// but maybe some backends might want to utilize the segregation
	RemoveDirUnderInode(db *sql.DB, inode int32, name string) error
	RemoveFileUnderInode(db *sql.DB, inode int32, name string) error
}

type defaultBackend struct{}

// VerifyDB does pretty basic check for whether the necessary tables were created.
// This check might pass and later operations still might fail.
//
// TODO: do more extensive checks
func (d defaultBackend) VerifyDB(db *sql.DB) error {
	var rootName string
	err := db.QueryRow(db.Rebind("select name from metadata where inode = ?"), 1).Scan(&rootName)
	if err != nil {
		return err
	}

	if rootName != "" {
		return errors.New("Expected to find entry with empty name for inode=1 in metadata")
	}

	return nil
}

// InitializeDBRows creates the necessary rows for fs to function
//
// Currently, only root metadata is setup
func (d defaultBackend) InitializeDBRows(db *sql.DB) error {
	tx, err := db.Beginx()
	if err != nil {
		log.Println("Couldn't prepare tx for initial rows!")
		return err
	}

	// add metadata entries for /
	var currentTimeNs = time.Now().UnixNano()
	_, err = tx.Exec(db.Rebind(
		`insert into
            metadata(inode,uid,gid,mode,type,ctime,atime,mtime,name)
            values (?, ?, ?, ?, ?, ?, ?, ?, ?)`),
		1, os.Getuid(), os.Getgid(), os.ModeDir|0755, fuse.DT_Dir, currentTimeNs, currentTimeNs, currentTimeNs, "",
	)
	if err != nil {
		log.Println("Couldn't insert metadata rows!")
		return err
	}

	err = tx.Commit()
	if err != nil {
		log.Println("Couldn't commit tx for initial rows!")
		return err
	}

	return nil
}

// GetMetadataForInode retrieves metadata for a given inode from db
func (d defaultBackend) GetMetadataForInode(db *sql.DB, inode int32) (Metadata, error) {
	var metadata Metadata

	err := db.QueryRowx(db.Rebind(
		`select
            inode,uid,gid,mode,type,ctime,atime,mtime,name,size
            from metadata where inode = ?`), inode,
	).StructScan(&metadata)
	if err != nil {
		log.Println("Coulnd't query select statement for Attr lookup")
		return Metadata{}, err
	}

	return metadata, nil
}

// SetMetadataForInode updates metadata for inode on db
func (d defaultBackend) SetMetadataForInode(db *sql.DB, inode int32, metadata Metadata) error {
	tx, err := db.Beginx()
	if err != nil {
		log.Println("Couldn't prepare tx for metadata update!")
		return err
	}
	_, err = tx.Exec(db.Rebind(
		`update metadata
            set uid = ?, gid = ?, mode = ?, type = ?, ctime = ?, atime = ?, mtime = ?, name = ?, size = ?
            where inode = ?`),
		metadata.Uid, metadata.Gid, metadata.Mode, metadata.Type, metadata.Ctime,
		metadata.Atime, metadata.Mtime, metadata.Name, metadata.Size, metadata.Inode,
	)
	if err != nil {
		log.Println("Couldn't update data for metadata row!")
		return err
	}

	err = tx.Commit()
	if err != nil {
		log.Println("Couldn't commit tx for metadata update!")
		return err
	}

	return nil
}

// GetDirectoryContentsForInode returns all children of inode from db
func (d defaultBackend) GetDirectoryContentsForInode(db *sql.DB, inode int32) ([]int32, error) {
	var childInodes []int32

	rows, err := db.Query(db.Rebind("select inode from parent where pinode = ?"), inode)
	if err != nil {
		log.Println("Couldn't query parent table!")
		return childInodes, err
	}
	defer rows.Close()
	for rows.Next() {
		var childInode int64
		err = rows.Scan(&childInode)
		if err != nil {
			log.Println("Couldn't query child inodes!")
			return childInodes, err
		}

		childInodes = append(childInodes, int32(childInode))
	}
	err = rows.Err()
	if err != nil {
		log.Println("Couldn't query child inodes!")
		return childInodes, err
	}

	return childInodes, nil
}

// GetFileContentsForInode reads file contents for inode from db
//
// TODO: split contents into blocks
func (d defaultBackend) GetFileContentsForInode(db *sql.DB, inode int32) ([]byte, error) {
	var data []byte
	var size int64

	err := db.QueryRow(db.Rebind("select size from metadata where inode = ?"), inode).Scan(&size)
	if err != nil {
		log.Println("Couldn't get metadata!")
		return data, err
	}

	err = db.QueryRow(db.Rebind("select data from filedata where inode = ?"), inode).Scan(&data)
	if err != nil {
		log.Println("Couldn't get filedata!")
		return data, err
	}

	tx, err := db.Beginx()
	if err != nil {
		log.Println("Couldn't prepare tx for metadata update!")
		return data, err
	}

	var currentTimeNs = time.Now().UnixNano()
	_, err = tx.Exec(db.Rebind("update metadata set atime = ? where inode = ?"), currentTimeNs, inode)
	if err != nil {
		log.Println("Couldn't update data for metadata row!")
		return data, err
	}

	err = tx.Commit()
	if err != nil {
		log.Println("Couldn't commit tx for metadata update!")
		return data, err
	}

	return data[:size], nil
}

// SetFileContentsForInode updates file content for inode on db
func (d defaultBackend) SetFileContentsForInode(db *sql.DB, inode int32, data []byte) error {
	tx, err := db.Beginx()
	if err != nil {
		log.Println("Couldn't prepare tx for filedata update!")
		return err
	}

	_, err = tx.Exec(db.Rebind("update filedata set data = ? where inode = ?"), data, inode)
	if err != nil {
		log.Println("Couldn't update data for filedata row!")
		return err
	}

	var currentTimeNs = time.Now().UnixNano()
	_, err = tx.Exec(db.Rebind("update metadata set size = ?, mtime = ? where inode = ?"), len(data), currentTimeNs, inode)
	if err != nil {
		log.Println("Couldn't update data for metadata row!")
		return err
	}

	err = tx.Commit()
	if err != nil {
		log.Println("Couldn't commit tx for filedata update!")
		return err
	}

	return nil
}

// CreateDirUnderInode creates a Dir named name under directory referred to by inode
func (d defaultBackend) CreateDirUnderInode(db *sql.DB, inode int32, name string) (int32, error) {
	tx, err := db.Beginx()
	if err != nil {
		log.Println("Couldn't prepare tx for mkdir!")
		return 0, err
	}

	newDirInode, err := insertIntoMetadata(tx, int64(os.ModeDir|0755), int64(fuse.DT_Dir), name)
	if err != nil {
		return 0, err
	}

	err = insertIntoParent(tx, int64(inode), int64(newDirInode))
	if err != nil {
		return 0, err
	}

	err = tx.Commit()
	if err != nil {
		log.Println("Couldn't commit tx for mkdir!")
		return 0, err
	}

	return int32(newDirInode), nil
}

// CreateFileUnderInode creates a File named name under directory referred to by inode
func (d defaultBackend) CreateFileUnderInode(db *sql.DB, inode int32, name string) (int32, error) {
	tx, err := db.Beginx()
	if err != nil {
		log.Println("Couldn't prepare tx for create file!")
		return 0, err
	}

	newFileInode, err := insertIntoMetadata(tx, int64(0644), int64(fuse.DT_File), name)
	if err != nil {
		return 0, err
	}

	_, err = tx.Exec(db.Rebind("insert into filedata values (?, ?)"), newFileInode, []byte(""))
	if err != nil {
		log.Println("Couldn't insert create file data rows!")
		return 0, err
	}

	err = insertIntoParent(tx, int64(inode), int64(newFileInode))
	if err != nil {
		return 0, err
	}

	err = tx.Commit()
	if err != nil {
		log.Println("Couldn't commit tx for create file!")
		return 0, err
	}

	return int32(newFileInode), nil
}

// RemoveDirUnderInode removes Dir named name from  directory referred to by inode
func (d defaultBackend) RemoveDirUnderInode(db *sql.DB, inode int32, name string) error {
	childInode, err := getInodeFromNameUnderDir(db, inode, name)
	if err != nil {
		log.Println("Couldn't retrieve inode from name!")
		return err
	}

	var nChildren int64
	err = db.QueryRow(db.Rebind("select count(*) from parent where pinode = ?"), childInode).Scan(&nChildren)
	if err != nil {
		log.Println("Couldn't retrive children for inode!")
	}

	if nChildren > 0 {
		return fuse.Errno(syscall.ENOTEMPTY)
	}

	tx, err := db.Beginx()
	if err != nil {
		log.Println("Couldn't prepare tx for dir removal!")
		return err
	}

	// delete from metadata
	err = removeFromMetadata(tx, int64(childInode))
	if err != nil {
		return err
	}

	// delete from parent
	err = removeFromParent(tx, int64(inode), int64(childInode))
	if err != nil {
		return err
	}

	err = tx.Commit()
	if err != nil {
		log.Println("Couldn't commit tx for dir removal!")
		return err
	}

	return nil
}

// RemoveFileUnderInode removes File named name from  directory referred to by inode
func (d defaultBackend) RemoveFileUnderInode(db *sql.DB, inode int32, name string) error {
	childInode, err := getInodeFromNameUnderDir(db, inode, name)
	if err != nil {
		log.Println("Couldn't retrieve inode from name!")
		return err
	}

	tx, err := db.Beginx()
	if err != nil {
		log.Println("Couldn't prepare tx for file removal!")
		return err
	}

	// delete from metadata
	err = removeFromMetadata(tx, int64(childInode))
	if err != nil {
		return err
	}

	// delete from filedata
	_, err = tx.Exec(db.Rebind("delete from filedata where inode = ?"), childInode)
	if err != nil {
		log.Println("Couldn't remove filedata rows!")
		return err
	}

	// delete from parent
	err = removeFromParent(tx, int64(inode), int64(childInode))
	if err != nil {
		return err
	}

	err = tx.Commit()
	if err != nil {
		log.Println("Couldn't commit tx for file removal!")
		return err
	}

	return nil
}
