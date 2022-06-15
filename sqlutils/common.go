package sqlutils

import (
	"database/sql"
	"log"
	"os"
	"syscall"
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

func (d defaultBackend) InitializeDBRows(db *sql.DB) error {
	tx, err := db.Begin()
	if err != nil {
		log.Println("Couldn't prepare tx for initial rows!")
		return err
	}

	// add metadata entries for / and /testfile
	metadataStmt, err := tx.Prepare(`insert into 
        metadata(inode,uid,gid,mode,type,ctime,atime,mtime,name)
        values (?, ?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		log.Println("Couldn't create prepared statement for metadata rows!")
		return err
	}
	defer metadataStmt.Close()

	var currentTimeNs = time.Now().UnixNano()
	_, err = metadataStmt.Exec(1, os.Getuid(), os.Getgid(), os.ModeDir|0755, fuse.DT_Dir, currentTimeNs, currentTimeNs, currentTimeNs, "")
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

func (d defaultBackend) GetMetadataForInode(db *sql.DB, inode int32) (Metadata, error) {
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

func (d defaultBackend) GetDirectoryContentsForInode(db *sql.DB, inode int32) ([]int32, error) {
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

func (d defaultBackend) GetFileContentsForInode(db *sql.DB, inode int32) ([]byte, error) {
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

	metadataStmt, err := tx.Prepare("update metadata set atime = ? where inode = ?")
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

func (d defaultBackend) SetFileContentsForInode(db *sql.DB, inode int32, data []byte) error {
	tx, err := db.Begin()
	if err != nil {
		log.Println("Couldn't prepare tx for filedata update!")
		return err
	}

	dataStmt, err := tx.Prepare("update filedata set data = ? where inode = ?")
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

	metadataStmt, err := tx.Prepare("update metadata set size = ?, mtime = ? where inode = ?")
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

func (d defaultBackend) CreateDirUnderInode(db *sql.DB, inode int32, name string) (int32, error) {
	tx, err := db.Begin()
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

func (d defaultBackend) CreateFileUnderInode(db *sql.DB, inode int32, name string) (int32, error) {
	tx, err := db.Begin()
	if err != nil {
		log.Println("Couldn't prepare tx for create file!")
		return 0, err
	}

	newFileInode, err := insertIntoMetadata(tx, int64(0644), int64(fuse.DT_File), name)
	if err != nil {
		return 0, err
	}

	contentStmt, err := tx.Prepare("insert into filedata values (?, ?)")
	if err != nil {
		log.Println("Couldn't create prepared statement for create file data rows!")
		return 0, err
	}
	defer contentStmt.Close()
	_, err = contentStmt.Exec(newFileInode, []byte(""))
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

func (d defaultBackend) RemoveDirUnderInode(db *sql.DB, inode int32, name string) error {
	childInode, err := getInodeFromNameUnderDir(db, inode, name)
	if err != nil {
		log.Println("Couldn't retrieve inode from name!")
		return err
	}

	var nChildren int64
	err = db.QueryRow("select count(*) from parent where pinode = ?", childInode).Scan(&nChildren)
	if err != nil {
		log.Println("Couldn't retrive children for inode!")
	}

	if nChildren > 0 {
		return fuse.Errno(syscall.ENOTEMPTY)
	}

	tx, err := db.Begin()
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

func (d defaultBackend) RemoveFileUnderInode(db *sql.DB, inode int32, name string) error {
	childInode, err := getInodeFromNameUnderDir(db, inode, name)
	if err != nil {
		log.Println("Couldn't retrieve inode from name!")
		return err
	}

	tx, err := db.Begin()
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
	contentStmt, err := tx.Prepare("delete from filedata where inode = ?")
	if err != nil {
		log.Println("Couldn't create prepared statement for filedata removal!")
		return err
	}
	defer contentStmt.Close()
	_, err = contentStmt.Exec(childInode)
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

func getInodeFromNameUnderDir(db *sql.DB, parentInode int32, name string) (int32, error) {
	var childInode int64
	err := db.QueryRow(`select parent.inode
        from parent
        left join metadata on parent.inode = metadata.Inode
        where pinode = ? and name = ?`, parentInode, name).Scan(&childInode)
	if err != nil {
		log.Printf("Couldn't retrieve inode from name!\n")
		return 0, err
	}

	return int32(childInode), nil
}

func insertIntoMetadata(tx *sql.Tx, mode, type_ int64, name string) (int32, error) {
	metadataStmt, err := tx.Prepare(`insert into
        metadata(uid,gid,mode,type,ctime,atime,mtime,name)
        values (?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		log.Println("Couldn't create prepared statement for mkdir metadata rows!")
		return 0, err
	}
	defer metadataStmt.Close()

	var currentTimeNs = time.Now().UnixNano()
	result, err := metadataStmt.Exec(os.Getuid(), os.Getgid(), mode, type_, currentTimeNs, currentTimeNs, currentTimeNs, name)
	if err != nil {
		log.Println("Couldn't insert mkdir metadata rows!")
		return 0, err
	}
	newInode, err := result.LastInsertId()
	if err != nil {
		log.Println("SQL Backend doesn't support sending LastInsertId :(")
		return 0, err
	}

	return int32(newInode), nil
}

func insertIntoParent(tx *sql.Tx, parentInode, childInode int64) error {
	parentStmt, err := tx.Prepare("insert into parent values (?, ?)")
	if err != nil {
		log.Println("Couldn't create prepared statement for mkdir parent rows!")
		return err
	}
	defer parentStmt.Close()

	_, err = parentStmt.Exec(parentInode, childInode)
	if err != nil {
		log.Println("Couldn't insert mkdir parent rows!")
		return err
	}

	return nil
}

func removeFromMetadata(tx *sql.Tx, inode int64) error {
	metadataStmt, err := tx.Prepare("delete from metadata where inode = ?")
	if err != nil {
		log.Println("Couldn't create prepared statement for metadata removal!")
		return err
	}
	defer metadataStmt.Close()
	_, err = metadataStmt.Exec(inode)
	if err != nil {
		log.Println("Couldn't remove file metadata rows!")
		return err
	}

	return nil
}

func removeFromParent(tx *sql.Tx, parentInode, childInode int64) error {
	parentStmt, err := tx.Prepare("delete from parent where pinode = ? and inode = ?")
	if err != nil {
		log.Println("Couldn't create prepared statement for parent row removal!")
		return err
	}
	defer parentStmt.Close()
	_, err = parentStmt.Exec(parentInode, childInode)
	if err != nil {
		log.Println("Couldn't remove parent rows!")
		return err
	}

	return nil
}
