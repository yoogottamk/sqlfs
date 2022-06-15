package sqlutils

import (
	"database/sql"
	"log"
	"os"
	"time"

	"bazil.org/fuse"
)

// metadata table as go struct
type Metadata struct {
	Inode int64

	Uid int64
	Gid int64

	Mode int64
	Type int64

	Ctime int64
	Atime int64
	Mtime int64

	Name string
	Size int64
}

type SQLBackend interface {
	OpenDB(dsn string) (*sql.DB, error)
	VerifyDB(db *sql.DB) error
	CreateDBTables(db *sql.DB) error
	InitializeDBRows(db *sql.DB) error
	GetMetadataForInode(db *sql.DB, inode int32) (Metadata, error)
	GetDirectoryContentsForInode(db *sql.DB, inode int32) ([]int32, error)
	GetFileContentsForInode(db *sql.DB, inode int32) ([]byte, error)
	SetFileContentsForInode(db *sql.DB, inode int32, data []byte) error
}

type DefaultBackend struct{}

func (d DefaultBackend) OpenDB(dsn string) (*sql.DB, error) {
	panic("DefaultBackend should not be used!")
}

func (d DefaultBackend) InitializeDBRows(db *sql.DB) error {
	var defaultFileContents = []byte("Hello, World!\n")

	tx, err := db.Begin()
	if err != nil {
		log.Println("Couldn't prepare tx for initial rows!")
		return err
	}

	// add metadata entries for / and /testfile
	metadataStmt, err := tx.Prepare(`insert into 
        metadata(inode,uid,gid,mode,type,ctime,atime,mtime,name,size) 
        values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		log.Println("Couldn't create prepared statement for metadata rows!")
		return err
	}
	defer metadataStmt.Close()

	var currentTimeNs = time.Now().UnixNano()
	_, err = metadataStmt.Exec(1, os.Getuid(), os.Getgid(), os.ModeDir|0755, fuse.DT_Dir, currentTimeNs, currentTimeNs, currentTimeNs, "/", 0)
	if err != nil {
		log.Println("Couldn't insert metadata rows!")
		return err
	}
	_, err = metadataStmt.Exec(2, os.Getuid(), os.Getgid(), 0644, fuse.DT_File, currentTimeNs, currentTimeNs, currentTimeNs, "testfile", len(defaultFileContents))
	if err != nil {
		log.Println("Couldn't insert metadata rows!")
		return err
	}

	// add contents for /testfile
	contentStmt, err := tx.Prepare(`insert into filedata values (?, ?)`)
	if err != nil {
		log.Println("Couldn't create prepared statement for filedata rows!")
		return err
	}
	defer contentStmt.Close()

	_, err = contentStmt.Exec(2, defaultFileContents)
	if err != nil {
		log.Println("Couldn't insert filedata rows!")
		return err
	}

	// add contents for parent table
	parentStmt, err := tx.Prepare(`insert into parent values (?, ?)`)
	if err != nil {
		log.Println("Couldn't create prepared statement for parent rows!")
		return err
	}
	defer parentStmt.Close()

	_, err = parentStmt.Exec(1, 2)
	if err != nil {
		log.Println("Couldn't insert parent rows!")
		return err
	}

	err = tx.Commit()
	if err != nil {
		log.Println("Couldn't commit tx for initial rows!")
		return err
	}

	return nil
}

func (d DefaultBackend) GetMetadataForInode(db *sql.DB, inode int32) (Metadata, error) {
	stmt, err := db.Prepare(
		`select 
        inode,uid,gid,mode,type,ctime,atime,mtime,name,size 
        from metadata where inode = ?`,
	)
	if err != nil {
		log.Println("Couldn't prepare select statement for dir Attr lookup")
		return Metadata{}, err
	}
	defer stmt.Close()

	var uid int64
	var gid int64
	var mode int64
	var type_ int64
	var ctime int64
	var atime int64
	var mtime int64
	var name string
	var size int64
	err = stmt.QueryRow(inode).Scan(&inode, &uid, &gid, &mode, &type_, &ctime, &atime, &mtime, &name, &size)
	if err != nil {
		log.Println("Coulnd't query select statement for dir Attr lookup")
		return Metadata{}, err
	}

	return Metadata{
		Inode: int64(inode),
		Uid:   uid,
		Gid:   gid,
		Mode:  mode,
		Type:  type_,
		Ctime: ctime,
		Atime: atime,
		Mtime: mtime,
		Name:  name,
		Size:  size,
	}, nil
}

func (d DefaultBackend) GetDirectoryContentsForInode(db *sql.DB, inode int32) ([]int32, error) {
	var childInodes []int32

	rows, err := db.Query("select inode from parent where pinode = ?", inode)
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

func (d DefaultBackend) GetFileContentsForInode(db *sql.DB, inode int32) ([]byte, error) {
	var data []byte

	stmt, err := db.Prepare("select data from filedata where inode = ?")
	if err != nil {
		log.Println("Couldn't prepare statement to get filedata!")
		return data, err
	}
	defer stmt.Close()

	err = stmt.QueryRow(inode).Scan(&data)
	if err != nil {
		log.Println("Couldn't get filedata!")
		return data, err
	}

	tx, err := db.Begin()
	if err != nil {
		log.Println("Couldn't prepare tx for metadata update!")
		return data, err
	}

	metadataStmt, err := tx.Prepare(`update metadata set atime = ? where inode = ?`)
	if err != nil {
		log.Println("Couldn't create prepared statement for metadata update!")
		return data, err
	}
	defer metadataStmt.Close()

	var currentTimeNs = time.Now().UnixNano()
	_, err = metadataStmt.Exec(currentTimeNs, inode)
	if err != nil {
		log.Println("Couldn't update data for metadata row!")
		return data, err
	}

	err = tx.Commit()
	if err != nil {
		log.Println("Couldn't commit tx for metadata update!")
		return data, err
	}

	return data, nil
}

func (d DefaultBackend) SetFileContentsForInode(db *sql.DB, inode int32, data []byte) error {
	tx, err := db.Begin()
	if err != nil {
		log.Println("Couldn't prepare tx for filedata update!")
		return err
	}

	dataStmt, err := tx.Prepare(`update filedata set data = ? where inode = ?`)
	if err != nil {
		log.Println("Couldn't create prepared statement for filedata update!")
		return err
	}
	defer dataStmt.Close()

	_, err = dataStmt.Exec(data, inode)
	if err != nil {
		log.Println("Couldn't update data for filedata row!")
		return err
	}

	metadataStmt, err := tx.Prepare(`update metadata set size = ?, mtime = ? where inode = ?`)
	if err != nil {
		log.Println("Couldn't create prepared statement for metadata update!")
		return err
	}
	defer metadataStmt.Close()

	var currentTimeNs = time.Now().UnixNano()
	_, err = metadataStmt.Exec(len(data), currentTimeNs, inode)
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
