package fuse

import (
	"log"
	"os"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
)

func MountFS(dsn, mountpoint string) error {
	db, err := backend.OpenDB(dsn)
	if err != nil {
		log.Println("Couldn't open sqlite3 file!")
		return err
	}

	if _, err := os.Stat(dsn); err != nil {
		log.Printf("File %s does not exist. Creating...\n", dsn)
		err = backend.CreateDBTables(db)
		if err != nil {
			return err
		}
		err = backend.InitializeDBRows(db)
		if err != nil {
			return err
		}
	}

	// verify whether its usable
	if err := backend.VerifyDB(db); err != nil {
		log.Println("SQL file Verification failed!")
		return err
	}

	c, err := fuse.Mount(mountpoint)
	if err != nil {
		return err
	}
	defer c.Close()

	filesys := &FS{db}
	if err = fs.Serve(c, filesys); err != nil {
		return err
	}

	<-c.Ready
	if err = c.MountError; err != nil {
		return err
	}

	return nil
}
