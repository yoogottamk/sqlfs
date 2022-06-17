package cmd

import (
	"io/fs"
	"io/ioutil"
	"os"
	"testing"

	"bazil.org/fuse/fs/fstestutil"

	"github.com/yoogottamk/sqlfs/pkg/fuse"
	"github.com/yoogottamk/sqlfs/pkg/sqlutils"
)

func assertFileSizeIs(t *testing.T, filepath string, expectedSize int64) {
	t.Helper()

	fileinfo, err := os.Stat(filepath)
	if err != nil {
		t.Fatalf("Couldn't stat file: %v", err)
	}

	fsSize := fileinfo.Size()
	if fsSize != expectedSize {
		t.Fatalf("Size on fs[%d] doesn't match expected size[%d]", fsSize, expectedSize)
	}
}

func getMountedFS(t *testing.T, backend sqlutils.SQLBackend) *fstestutil.Mount {
	fuse.Backend = backend

	dsn := t.TempDir() + "/fs.sql"

	t.Logf("Using dsn '%s'", dsn)

	db, err := backend.OpenDB(dsn)
	if err != nil {
		t.Fatalf("Couldn't open db[%s]: %v", dsn, err)
	}

	err = backend.CreateDBTables(db)
	if err != nil {
		t.Fatalf("Couldn't create tables: %v", err)
	}

	err = backend.InitializeDBRows(db)
	if err != nil {
		t.Fatalf("Couldn't create initial rows: %v", err)
	}

	filesys := fuse.FS{Db: db}
	mnt, err := fstestutil.MountedT(t, &filesys, nil)
	if err != nil {
		t.Fatalf("Couldn't mount sqlfs: %v", err)
	}

	return mnt
}

func testBasicFileOperations(t *testing.T, mnt *fstestutil.Mount) {
	mountedDir := mnt.Dir

	testfile := mountedDir + "/testfile"
	initialContents := "Hello!"

	// create file
	err := ioutil.WriteFile(testfile, []byte(initialContents), fs.FileMode(0644))
	if err != nil {
		t.Fatalf("Couldn't write to file: %v", err)
	}

	assertFileSizeIs(t, testfile, int64(len(initialContents)))

	// read from file
	contents, err := ioutil.ReadFile(testfile)
	if err != nil {
		t.Fatalf("Couldn't read from file: %v", err)
	}
	if string(contents) != initialContents {
		t.Fatalf("Wrong contents read from file")
	}

	// append to file
	f, err := os.OpenFile(testfile, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatalf("Couldn't open file: %v", err)
	}
	if _, err = f.WriteString(initialContents); err != nil {
		t.Fatalf("Couldn't write to file: %v", err)
	}
	if err = f.Close(); err != nil {
		t.Fatalf("Couldn't close file: %v", err)
	}

	// verify size
	assertFileSizeIs(t, testfile, 2*int64(len(initialContents)))

	// truncate file
	err = os.Truncate(testfile, 0)
	if err != nil {
		t.Fatalf("Couldn't truncate file: %v", err)
	}

	// verify size
	assertFileSizeIs(t, testfile, 0)
}

func TestBasicMountSqlite(t *testing.T) {
	mnt := getMountedFS(t, sqlutils.SQLiteBackend{})
	mnt.Close()
}

func TestBasicFileOperationsSqlite(t *testing.T) {
	mnt := getMountedFS(t, sqlutils.SQLiteBackend{})
	defer mnt.Close()

	testBasicFileOperations(t, mnt)
}
