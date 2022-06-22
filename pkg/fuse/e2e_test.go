package fuse

import (
	"testing"

	"github.com/yoogottamk/sqlfs/pkg/sqlutils"
)

type backendTestSpec struct {
	dsn     string
	backend sqlutils.SQLBackend
	name    string
}

func getTestingBackends(t *testing.T) []backendTestSpec {
	return []backendTestSpec{
		{
			dsn:     t.TempDir() + "/fs.sql",
			backend: sqlutils.SQLiteBackend{},
			name:    "sqlite",
		},
		{
			dsn:     setupMySQLContainer(t),
			backend: sqlutils.MySQLBackend{},
			name:    "mysql",
		},
		{
			dsn:     setupPostgresContainer(t),
			backend: sqlutils.PostgresBackend{},
			name:    "postgres",
		},
	}
}

func TestBasicMount(t *testing.T) {
	for _, tc := range getTestingBackends(t) {
		t.Run(tc.name, func(t *testing.T) {
			mnt := getMountedFS(t, tc.backend, tc.dsn)
			mnt.Close()
		})
	}
}

func TestBasicFileOperations(t *testing.T) {
	for _, tc := range getTestingBackends(t) {
		t.Run(tc.name, func(t *testing.T) {
			mnt := getMountedFS(t, tc.backend, tc.dsn)
			defer mnt.Close()

			testBasicFileOperations(t, mnt)
		})
	}

}

func TestBasicDirectoryOperations(t *testing.T) {
	for _, tc := range getTestingBackends(t) {
		t.Run(tc.name, func(t *testing.T) {
			mnt := getMountedFS(t, tc.backend, tc.dsn)
			defer mnt.Close()

			testBasicDirOperations(t, mnt)
		})
	}

}
