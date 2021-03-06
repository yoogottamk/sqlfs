package sqlutils

import (
	_ "embed"
	"log"

	sql "github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
)

type SQLiteBackend struct{ defaultBackend }

var _ SQLBackend = (*SQLiteBackend)(nil)

//go:embed init-sqlite3.sql
var createTableSqlite3 string

// OpenDB connects to dsn
func (s SQLiteBackend) OpenDB(dsn string) (*sql.DB, error) {
	return sql.Open("sqlite3", dsn)
}

// CreateDBTables creates db tables using sql file
func (s SQLiteBackend) CreateDBTables(db *sql.DB) error {
	_, err := db.Exec(createTableSqlite3)
	if err != nil {
		log.Println("Couldn't write initial tables!")
		return err
	}

	return nil
}
