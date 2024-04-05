package dbbatch

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/jmoiron/sqlx"
)

var (
	ErrTxNotSupported       = errors.New("transaction is not supported in batch, use BeginBatchTx method")
	ErrNestedTxNotSupported = errors.New("nested transactions are not supported")
	ErrStmtNotSupported     = errors.New("prepared statements are not supported in batch, simple queries")
	ErrNoRunningBatch       = errors.New("connection has no running batch")
	ErrHasRunningBatch      = errors.New("connection has running batch")
)

type BatchDB struct {
	*sqlx.DB
	options options
}

func New(db *sqlx.DB, opts ...Option) *BatchDB {
	o := options{
		withoutCancel: false,
	}
	for _, opt := range opts {
		opt(&o)
	}
	return &BatchDB{
		DB:      db,
		options: o,
	}
}

func (bdb *BatchDB) maybeWithoutCancel(ctx context.Context) context.Context {
	if !bdb.options.withoutCancel {
		return ctx
	}

	return ContextWithoutCancel(ctx)
}

// BatchConn creates *BatchConn. Must call BatchConn.Close() in the end if err is nil
func (bdb *BatchDB) BatchConn(ctx context.Context) (bc *BatchConn, err error) {
	if BatchConnFromContext(ctx) != nil {
		return nil, errors.New("don't support nested batch")
	}

	conn, err := bdb.DB.Connx(ctx)
	if err != nil {
		return nil, fmt.Errorf("db.Conn(ctx): %w", err)
	}
	if conn == nil {
		return nil, errors.New("db.Conn(ctx) returned nil")
	}

	return newBatchConn(bdb, conn), nil
}

func (bdb *BatchDB) BeginBatchTx(ctx context.Context, opts *sql.TxOptions) (*BatchTx, error) {
	bc, err := bdb.BatchConn(ctx)
	if err != nil {
		return nil, fmt.Errorf("bdb.BatchConn: %w", err)
	}

	return bc.BeginBatchTx(ctx, opts)
}

func (bdb *BatchDB) SendBatch(ctx context.Context, b *Batch) (err error) {
	bc := BatchConnFromContext(ctx)
	if bc != nil {
		return bc.SendBatch(ctx, b)
	}

	bc, err = bdb.BatchConn(ctx)
	if err != nil {
		return fmt.Errorf("bdb.BatchConn: %w", err)
	}
	defer func() {
		_ = bc.Close()
	}()

	return bc.SendBatch(bdb.maybeWithoutCancel(ctx), b)
}

// overwrite all methods of DB with context
// except PrepareContext, PreparexContext, NamedPrepareContext
// (unsupported in batch, will get error from driver if there batch runner in context)
// Methods without context reused from sqlx.DB. PingContext reused from sqlx.DB

func (bdb *BatchDB) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	if bc := BatchConnFromContext(ctx); bc != nil {
		return bc.QueryContext(ctx, query, args...)
	}

	// waiting conn cancelling optimization not implemented as requires breaking API by returning extended row
	return bdb.DB.QueryContext(bdb.maybeWithoutCancel(ctx), query, args...)
}

func (bdb *BatchDB) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	if bc := BatchConnFromContext(ctx); bc != nil {
		return bc.ExecContext(ctx, query, args...)
	}

	// optimization: waiting conn can be canceled
	if bdb.options.withoutCancel {
		conn, err := bdb.DB.Conn(ctx)
		if err != nil {
			return nil, err
		}
		defer conn.Close()

		return conn.ExecContext(ContextWithoutCancel(ctx), query, args...)
	}

	return bdb.DB.ExecContext(ctx, query, args...)
}

func (bdb *BatchDB) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	if bc := BatchConnFromContext(ctx); bc != nil {
		return bc.QueryRowContext(ctx, query, args...)
	}

	// waiting conn cancelling optimization not implemented as requires breaking API by returning extended row
	return bdb.DB.QueryRowContext(bdb.maybeWithoutCancel(ctx), query, args...)
}

func (bdb *BatchDB) QueryxContext(ctx context.Context, query string, args ...any) (*sqlx.Rows, error) {
	if bc := BatchConnFromContext(ctx); bc != nil {
		return bc.QueryxContext(ctx, query, args...)
	}

	// waiting conn cancelling optimization not implemented as requires breaking API by returning extended row
	return bdb.DB.QueryxContext(bdb.maybeWithoutCancel(ctx), query, args...)
}

func (bdb *BatchDB) QueryRowxContext(ctx context.Context, query string, args ...any) *sqlx.Row {
	if bc := BatchConnFromContext(ctx); bc != nil {
		return bc.QueryRowxContext(ctx, query, args...)
	}

	// waiting conn cancelling optimization not implemented as requires breaking API by returning extended row
	return bdb.DB.QueryRowxContext(bdb.maybeWithoutCancel(ctx), query, args...)
}

func (bdb *BatchDB) MustExecContext(ctx context.Context, query string, args ...any) sql.Result {
	if bc := BatchConnFromContext(ctx); bc != nil {
		return bc.MustExecContext(ctx, query, args...)
	}

	// optimization: waiting conn can be canceled
	if bdb.options.withoutCancel {
		conn, err := bdb.DB.Connx(ctx)
		if err != nil {
			panic(err)
		}
		defer conn.Close()

		return sqlx.MustExecContext(ContextWithoutCancel(ctx), conn, query, args...)
	}

	return bdb.DB.MustExecContext(ctx, query, args...)
}

func (bdb *BatchDB) GetContext(ctx context.Context, dest any, query string, args ...any) error {
	if bc := BatchConnFromContext(ctx); bc != nil {
		return bc.GetContext(ctx, dest, query, args...)
	}

	// optimization: waiting conn can be canceled
	if bdb.options.withoutCancel {
		conn, err := bdb.DB.Connx(ctx)
		if err != nil {
			return err
		}
		defer conn.Close()

		return conn.GetContext(ContextWithoutCancel(ctx), dest, query, args...)
	}

	return bdb.DB.GetContext(ctx, dest, query, args...)
}

func (bdb *BatchDB) SelectContext(ctx context.Context, dest any, query string, args ...any) error {
	if bc := BatchConnFromContext(ctx); bc != nil {
		return bc.SelectContext(ctx, dest, query, args...)
	}

	// optimization: waiting conn can be canceled
	if bdb.options.withoutCancel {
		conn, err := bdb.DB.Connx(ctx)
		if err != nil {
			return err
		}
		defer conn.Close()

		return conn.SelectContext(ContextWithoutCancel(ctx), dest, query, args...)
	}

	return bdb.DB.SelectContext(ctx, dest, query, args...)
}

func (bdb *BatchDB) NamedQueryContext(ctx context.Context, query string, arg any) (*sqlx.Rows, error) {
	if bc := BatchConnFromContext(ctx); bc != nil {
		return bc.NamedQueryContext(ctx, query, arg)
	}

	return bdb.DB.NamedQueryContext(bdb.maybeWithoutCancel(ctx), query, arg)
}

func (bdb *BatchDB) NamedExecContext(ctx context.Context, query string, arg any) (sql.Result, error) {
	if bc := BatchConnFromContext(ctx); bc != nil {
		return bc.NamedExecContext(ctx, query, arg)
	}

	// optimization: waiting conn can be canceled
	if bdb.options.withoutCancel {
		conn, err := bdb.DB.Connx(ctx)
		if err != nil {
			return nil, err
		}
		defer conn.Close()

		q, args, err := bdb.DB.BindNamed(query, arg)
		if err != nil {
			return nil, err
		}
		return conn.ExecContext(ctx, q, args...)
	}

	return bdb.DB.NamedExecContext(ctx, query, arg)
}

func (bdb *BatchDB) MustBeginTx(ctx context.Context, opts *sql.TxOptions) *sqlx.Tx {
	tx, err := bdb.BeginTxx(ctx, opts)
	if err != nil {
		panic(err)
	}
	return tx
}

func (bdb *BatchDB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	if bc := BatchConnFromContext(ctx); bc != nil {
		return nil, ErrTxNotSupported
	}

	return bdb.DB.BeginTx(bdb.maybeWithoutCancel(ctx), opts)
}

func (bdb *BatchDB) BeginTxx(ctx context.Context, opts *sql.TxOptions) (*sqlx.Tx, error) {
	if bc := BatchConnFromContext(ctx); bc != nil {
		return nil, ErrTxNotSupported
	}

	return bdb.DB.BeginTxx(bdb.maybeWithoutCancel(ctx), opts)
}

func (bdb *BatchDB) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	if bc := BatchConnFromContext(ctx); bc != nil {
		return nil, ErrStmtNotSupported
	}

	return bdb.DB.PrepareContext(ctx, query)
}

func (bdb *BatchDB) PreparexContext(ctx context.Context, query string) (*sqlx.Stmt, error) {
	if bc := BatchConnFromContext(ctx); bc != nil {
		return nil, ErrStmtNotSupported
	}

	return bdb.DB.PreparexContext(ctx, query)
}
