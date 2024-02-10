package dbbatch

import (
	"context"
	"database/sql"

	"github.com/jmoiron/sqlx"
)

//go:generate mockgen -source deps.go -package $GOPACKAGE -typed -destination mock_deps_test.go

type BaseConnProvider interface {
	BaseConn() interface{}
}

type BatchRequestsSender interface {
	SendBatchRequests(ctx context.Context, requests []Request) (res interface{}, close func() error, err error)
}

type BatchRunner interface {
	// Queue Only for using in the driver implementation code!
	Queue(request Request) interface{}
}

type batchRunnerMachine interface {
	run(ctx context.Context, b *Batch) (err error)
	Queue(request Request) interface{}
	roundTrip()
}

type Ext interface {
	sqlx.ExecerContext
	sqlx.QueryerContext
	sqlx.PreparerContext

	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	PreparexContext(ctx context.Context, query string) (*sqlx.Stmt, error)
	SelectContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error
	GetContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error
}
