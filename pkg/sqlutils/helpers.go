package sqlutils

import (
	"database/sql"
	"log"
	"os"
	"time"
)

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
	var currentTimeNs = time.Now().UnixNano()
	result, err := tx.Exec(`insert into
        metadata(uid,gid,mode,type,ctime,atime,mtime,name)
        values (?, ?, ?, ?, ?, ?, ?, ?)`,
		os.Getuid(), os.Getgid(), mode, type_, currentTimeNs, currentTimeNs, currentTimeNs, name,
	)
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
	_, err := tx.Exec("insert into parent values (?, ?)", parentInode, childInode)
	if err != nil {
		log.Println("Couldn't insert mkdir parent rows!")
		return err
	}

	return nil
}

func removeFromMetadata(tx *sql.Tx, inode int64) error {
	_, err := tx.Exec("delete from metadata where inode = ?", inode)
	if err != nil {
		log.Println("Couldn't remove file metadata rows!")
		return err
	}

	return nil
}

func removeFromParent(tx *sql.Tx, parentInode, childInode int64) error {
	_, err := tx.Exec("delete from parent where pinode = ? and inode = ?", parentInode, childInode)
	if err != nil {
		log.Println("Couldn't remove parent rows!")
		return err
	}

	return nil
}
