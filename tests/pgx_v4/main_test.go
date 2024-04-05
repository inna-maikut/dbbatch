//go:build integration

package pgx_v4

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/stdlib"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"

	"github.com/inna-maikut/dbbatch"
	_ "github.com/inna-maikut/dbbatch/pgx_v4"
)

func setup(t *testing.T, withoutCancel bool) (context.Context, *dbbatch.BatchDB) {
	// port = 23340
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
	})

	db, err := connect()
	require.NoError(t, err)

	err = db.PingContext(ctx)
	require.NoError(t, err)

	_, err = db.Exec("drop table if exists items")
	_, err = db.Exec(`create table items (
		id bigserial primary key,
		name text,
		user_id bigint not null,
		create_time timestamp with time zone default now()
    )`)
	require.NoError(t, err)

	return ctx, dbbatch.New(db, dbbatch.WithoutCancel(withoutCancel))
}

func connect() (*sqlx.DB, error) {
	// загружаем опции из окружения
	connConfig, err := pgx.ParseConfig("")
	if err != nil {
		return nil, err
	}

	connConfig.Host = "127.0.0.1"
	connConfig.Port = uint16(23340)
	connConfig.User = "postgres"
	connConfig.Password = "postgres"
	connConfig.Database = "master"

	configName := stdlib.RegisterConnConfig(connConfig)

	var db *sql.DB
	db, err = sql.Open("batch_pgx", configName)
	if err != nil {
		return nil, err
	}

	db.SetMaxOpenConns(5)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)
	db.SetConnMaxIdleTime(5 * time.Minute)

	if err = db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %s", err)
	}

	return sqlx.NewDb(db, "pgx"), nil
}
