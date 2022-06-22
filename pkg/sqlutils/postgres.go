package sqlutils

import (
	_ "embed"
	"log"

	sql "github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

type PostgresBackend struct{ defaultBackend }

var _ SQLBackend = (*MySQLBackend)(nil)

//go:embed init-postgres.sql
var createTablePostgres string

func (p PostgresBackend) OpenDB(dsn string) (*sql.DB, error) {
	return sql.Open("postgres", "postgres://"+dsn+"?sslmode=disable")
}

func (p PostgresBackend) CreateDBTables(db *sql.DB) error {
	_, err := db.Exec(createTablePostgres)
	if err != nil {
		log.Println("Couldn't write initial tables!")
		return err
	}

	return nil
}
