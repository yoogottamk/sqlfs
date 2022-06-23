package sqlutils

import (
	_ "embed"
	"log"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	sql "github.com/jmoiron/sqlx"
)

type MySQLBackend struct{ defaultBackend }

var _ SQLBackend = (*MySQLBackend)(nil)

//go:embed init-mysql.sql
var createTableMySql string

// OpenDB enables multiStatements and connects to dsn
func (m MySQLBackend) OpenDB(dsn string) (*sql.DB, error) {
	var querySep string

	if strings.Contains(dsn, "?") {
		// some options were already provided
		querySep = "&"
	} else {
		// no options were provided
		querySep = "?"
	}

	db, err := sql.Open("mysql", dsn+querySep+"multiStatements=true")
	if err != nil {
		return nil, err
	}

	db.SetConnMaxLifetime(time.Minute * 3)
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(10)

	return db, nil
}

// CreateDBTables creates db tables using sql file
func (m MySQLBackend) CreateDBTables(db *sql.DB) error {
	_, err := db.Exec(createTableMySql)
	if err != nil {
		log.Println("Couldn't write initial tables!")
		return err
	}

	return nil
}
