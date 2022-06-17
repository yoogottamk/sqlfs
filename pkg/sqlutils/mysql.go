package sqlutils

import (
	"database/sql"
	_ "embed"
	"log"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type MySQLBackend struct{ defaultBackend }

var _ SQLBackend = (*MySQLBackend)(nil)

//go:embed init-generic.sql
var createTableMySql string

func (m MySQLBackend) OpenDB(dsn string) (*sql.DB, error) {
	db, err := sql.Open("mysql", dsn+"?multiStatements=true")
	if err != nil {
		return nil, err
	}

	db.SetConnMaxLifetime(time.Minute * 3)
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(10)

	return db, nil
}

func (m MySQLBackend) CreateDBTables(db *sql.DB) error {
	_, err := db.Exec(createTableMySql)
	if err != nil {
		log.Println("Couldn't write initial tables!")
		return err
	}

	return nil
}
