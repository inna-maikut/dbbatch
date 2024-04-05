package dbbatch

import (
	"context"
	"database/sql"

	"github.com/jmoiron/sqlx"
)

//go:generate mockgen -source deps.go -package $GOPACKAGE -typed -destination mock_deps_test.go

type BaseConnProvider interface {
	BaseConn() any
}

type BatchRequestsSender interface {
	SendBatchRequests(ctx context.Context, requests []Request) (res any, close func() error, err error)
}

type BatchRunner interface {
	// Queue Only for using in the driver implementation code!
	Queue(request Request) any
}

type batchRunnerMachine interface {
	run(ctx context.Context, b *Batch) (err error)
	Queue(request Request) any
	roundTrip()
}

type Ext interface {
	sqlx.ExecerContext
	sqlx.QueryerContext
	sqlx.PreparerContext

	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	PreparexContext(ctx context.Context, query string) (*sqlx.Stmt, error)
	SelectContext(ctx context.Context, dest any, query string, args ...any) error
	GetContext(ctx context.Context, dest any, query string, args ...any) error
}
