package fuse

import (
	"database/sql"
	"log"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
)

func openDB(dsn string) (*sql.DB, error) {
	db, err := Backend.OpenDB(dsn)
	if err != nil {
		log.Println("Couldn't open DB!")
		return db, err
	}

	return db, nil
}

func InitializeDB(dsn string) error {
	db, err := openDB(dsn)
	if err != nil {
		return err
	}

	if err = Backend.CreateDBTables(db); err != nil {
		log.Println("Couldn't create DB tables!")
		return err
	}

	if err = Backend.InitializeDBRows(db); err != nil {
		log.Println("Couldn't insert initial rows!")
		return err
	}

	return nil
}

func VerifyDB(dsn string) error {
	db, err := openDB(dsn)
	if err != nil {
		return err
	}

	if err := Backend.VerifyDB(db); err != nil {
		log.Println("SQL DB Verification failed!")
		return err
	}

	return nil
}

func MountFS(dsn, mountpoint string) error {
	// verify whether its usable
	if err := VerifyDB(dsn); err != nil {
		return err
	}

	c, err := fuse.Mount(mountpoint)
	if err != nil {
		return err
	}
	defer c.Close()

	db, err := openDB(dsn)
	if err != nil {
		return err
	}

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
