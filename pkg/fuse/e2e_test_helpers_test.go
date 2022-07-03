package fuse

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"bazil.org/fuse/fs/fstestutil"
	"github.com/docker/go-connections/nat"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

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

func getMountedFS(t *testing.T, backend sqlutils.SQLBackend, dsn string) *fstestutil.Mount {
	Backend = backend

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

	filesys := FS{db}
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

	t.Run("write", func(t *testing.T) {
		if err := ioutil.WriteFile(testfile, []byte(initialContents), 0644); err != nil {
			t.Fatalf("Couldn't write to file: %v", err)
		}

		assertFileSizeIs(t, testfile, int64(len(initialContents)))
	})

	t.Run("read", func(t *testing.T) {
		contents, err := ioutil.ReadFile(testfile)
		if err != nil {
			t.Fatalf("Couldn't read from file: %v", err)
		}
		if string(contents) != initialContents {
			t.Fatalf("Wrong contents read from file")
		}
	})

	t.Run("append", func(t *testing.T) {
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
	})

	t.Run("truncate-read", func(t *testing.T) {
		err := os.Truncate(testfile, int64(len(initialContents)))
		if err != nil {
			t.Fatalf("Couldn't truncate file: %v", err)
		}

		// verify contents
		contents, err := ioutil.ReadFile(testfile)
		if err != nil {
			t.Fatalf("Couldn't read from file: %v", err)
		}
		if string(contents) != initialContents {
			t.Fatalf("Wrong contents read from file")
		}

		// verify size
		assertFileSizeIs(t, testfile, int64(len(initialContents)))
	})

	truncateFull := func(t *testing.T) {
		err := os.Truncate(testfile, 0)
		if err != nil {
			t.Fatalf("Couldn't truncate file: %v", err)
		}

		// verify size
		assertFileSizeIs(t, testfile, 0)
	}

	t.Run("truncate-full", truncateFull)

	t.Run("truncate-more", func(t *testing.T) {
		// set to 0
		truncateFull(t)

		// new size now
		newSize := 5
		err := os.Truncate(testfile, int64(newSize))
		if err != nil {
			t.Fatalf("Couldn't truncate file: %v", err)
		}

		// verify size
		assertFileSizeIs(t, testfile, int64(newSize))

		// verify contents
		data, err := ioutil.ReadFile(testfile)
		if data[0] != 0 {
			t.Fatalf("Unexpected file contents")
		}
		for i := 1; i < newSize; i++ {
			if data[i] != data[0] {
				t.Fatalf("Unexpected file contents")
			}
		}
	})
}

func testBasicDirOperations(t *testing.T, mnt *fstestutil.Mount) {
	mountedDir := mnt.Dir

	t.Run("mkdir", func(t *testing.T) {
		if err := os.MkdirAll(mountedDir+"/l1/l2/l3", 0755); err != nil {
			t.Fatalf("Couldn't create nested dir: %v", err)
		}
	})

	t.Run("rmdir", func(t *testing.T) {
		if err := os.Remove(mountedDir + "/l1/l2/l3"); err != nil {
			t.Fatalf("Couldn't remove dir: %v", err)
		}
	})

	t.Run("mkfile", func(t *testing.T) {
		if err := ioutil.WriteFile(mountedDir+"/l1/l2/testfile", []byte(""), 0644); err != nil {
			t.Fatalf("Couldn't create file inside dir: %v", err)
		}
	})

	t.Run("rmfile", func(t *testing.T) {
		if err := os.Remove(mountedDir + "/l1/l2/testfile"); err != nil {
			t.Fatalf("Couldn't remove file: %v", err)
		}
	})

	t.Run("rmdir-r", func(t *testing.T) {
		if err := os.RemoveAll(mountedDir + "/l1"); err != nil {
			t.Fatalf("Couldn't remove dir: %v", err)
		}
	})
}

func setupContainer(t *testing.T, image string, port nat.Port, env map[string]string, waitFor wait.Strategy) (string, string) {
	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Image:        image,
		ExposedPorts: []string{string(port)},
		Env:          env,
		// WaitingFor:   wait.ForListeningPort(port),
		WaitingFor: waitFor,
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("Couldn't start container: %v", err)
	}

	ip, err := container.Host(ctx)
	if err != nil {
		t.Fatalf("Couldn't get ip for container: %v", err)
	}

	mappedPort, err := container.MappedPort(ctx, port)
	if err != nil {
		t.Fatalf("Couldn't get mapped port for mysql container: %v", err)
	}

	// NOTE: not terminating container myself
	// this was done to simplify the testing interface
	//
	// relying on testcontainer's reaper
	// https://golang.testcontainers.org/features/garbage_collector/
	return ip, mappedPort.Port()
}

func setupMySQLContainer(t *testing.T) string {
	user := "user"
	password := "password"
	dbname := "sqlfs"

	port := nat.Port("3306/tcp")

	ip, mappedPort := setupContainer(t, "mariadb:latest", port, map[string]string{
		"MARIADB_USER":                 user,
		"MARIADB_PASSWORD":             password,
		"MARIADB_DATABASE":             dbname,
		"MARIADB_RANDOM_ROOT_PASSWORD": "yes",
	}, wait.ForListeningPort(port))

	return fmt.Sprintf("%s:%s@(%s:%s)/%s", user, password, ip, mappedPort, dbname)
}

func setupPostgresContainer(t *testing.T) string {
	user := "user"
	password := "password"
	dbname := "sqlfs"

	ip, mappedPort := setupContainer(t, "postgres:latest", nat.Port("5432/tcp"), map[string]string{
		"POSTGRES_USER":     user,
		"POSTGRES_PASSWORD": password,
		"POSTGRES_DB":       dbname,
	}, wait.ForLog("[1] LOG:  database system is ready to accept connections"))

	return fmt.Sprintf("%s:%s@%s:%s/%s", user, password, ip, mappedPort, dbname)
}
