package sqlutils

import (
	"log"
	"os"
	"time"

	sql "github.com/jmoiron/sqlx"
)

// getInodeFromNameUnderDir returns the inode of Dir/File under directory
// referred by parentInode from db
func getInodeFromNameUnderDir(db *sql.DB, parentInode int32, name string) (int32, error) {
	var childInode int64
	err := db.QueryRow(db.Rebind(
		`select parent.inode
            from parent
            left join metadata on parent.inode = metadata.Inode
            where pinode = ? and name = ?`),
		parentInode, name).Scan(&childInode)
	if err != nil {
		log.Printf("Couldn't retrieve inode from name!\n")
		return 0, err
	}

	return int32(childInode), nil
}

// insertIntoMetadata creates a metadata tbale row given mode, type_ and name
func insertIntoMetadata(tx *sql.Tx, mode, type_ int64, name string) (int32, error) {
	var inode int64
	// all backends don't provide LastInsertId
	tx.QueryRow(tx.Rebind("select 1 + max(inode) from metadata")).Scan(&inode)

	var currentTimeNs = time.Now().UnixNano()
	_, err := tx.Exec(tx.Rebind(
		`insert into
            metadata(inode, uid,gid,mode,type,ctime,atime,mtime,name)
            values (?, ?, ?, ?, ?, ?, ?, ?, ?)`),
		inode, os.Getuid(), os.Getgid(), mode, type_, currentTimeNs, currentTimeNs, currentTimeNs, name,
	)
	if err != nil {
		log.Printf("Couldn't insert metadata rows: %v\n", err)
		return 0, err
	}

	return int32(inode), nil
}

// insertIntoParent creates a parent table row given parentInode and childInode
func insertIntoParent(tx *sql.Tx, parentInode, childInode int64) error {
	_, err := tx.Exec(tx.Rebind("insert into parent values (?, ?)"), parentInode, childInode)
	if err != nil {
		log.Println("Couldn't insert mkdir parent rows!")
		return err
	}

	return nil
}

// removeFromMetadata removes row with inode from metadata table
func removeFromMetadata(tx *sql.Tx, inode int64) error {
	_, err := tx.Exec(tx.Rebind("delete from metadata where inode = ?"), inode)
	if err != nil {
		log.Println("Couldn't remove file metadata rows!")
		return err
	}

	return nil
}

// removeFromParent removes row with parentInode,childInode from parent table
//
// NOTE: this should never actually be required since foreign key ON DELETE should
//       take care of this
func removeFromParent(tx *sql.Tx, parentInode, childInode int64) error {
	_, err := tx.Exec(tx.Rebind("delete from parent where pinode = ? and inode = ?"),
		parentInode, childInode)
	if err != nil {
		log.Println("Couldn't remove parent rows!")
		return err
	}

	return nil
}
