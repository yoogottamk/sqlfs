package sqlutils

import (
	"database/sql"
	_ "embed"
	"log"

	_ "github.com/mattn/go-sqlite3"
)

type SQLiteBackend struct{ DefaultBackend }

var _ SQLBackend = (*SQLiteBackend)(nil)

//go:embed init-sqlite3.sql
var createTableSql string

func (s SQLiteBackend) OpenDB(dsn string) (*sql.DB, error) {
	return sql.Open("sqlite3", dsn)
}

func (s SQLiteBackend) VerifyDB(db *sql.DB) error {
	// TODO: check if loaded sql file is usable
	return nil
}

func (s SQLiteBackend) CreateDBTables(db *sql.DB) error {
	_, err := db.Exec(createTableSql)
	if err != nil {
		log.Println("Couldn't write initial tables!")
		return err
	}

	return nil
}
