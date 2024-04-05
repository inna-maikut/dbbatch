package dbbatch

import (
	"context"
	"database/sql"

	"github.com/jmoiron/sqlx"
)

type BatchTx struct {
	bc   *BatchConn
	tx   *sqlx.Tx
	done bool
}

func newBatchTx(bc *BatchConn, tx *sqlx.Tx) *BatchTx {
	return &BatchTx{
		bc:   bc,
		tx:   tx,
		done: false,
	}
}

// SendBatch sends batch in the transaction.
func (btx *BatchTx) SendBatch(ctx context.Context, b *Batch) error {
	if btx.done {
		return sql.ErrTxDone
	}
	return btx.bc.SendBatch(ctx, b)
}

// Commit commits the transaction and closes the connection.
func (btx *BatchTx) Commit() error {
	if btx.done {
		return sql.ErrTxDone
	}
	btx.done = true
	defer btx.bc.finishTx()
	return btx.tx.Commit()
}

// Rollback aborts the transaction and closes the connection.
func (btx *BatchTx) Rollback() error {
	if btx.done {
		return sql.ErrTxDone
	}
	btx.done = true
	defer btx.bc.finishTx()
	return btx.tx.Rollback()
}

// Add all methods of BatchDB except methods for beginning tx.

func (btx *BatchTx) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	if btx.done {
		return nil, sql.ErrTxDone
	}
	return btx.bc.QueryContext(ctx, query, args...)
}

func (btx *BatchTx) Query(query string, args ...any) (*sql.Rows, error) {
	if btx.done {
		return nil, sql.ErrTxDone
	}
	return btx.bc.Query(query, args...)
}

func (btx *BatchTx) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	if btx.done {
		return nil, sql.ErrTxDone
	}
	return btx.bc.ExecContext(ctx, query, args...)
}

func (btx *BatchTx) Exec(query string, args ...any) (sql.Result, error) {
	if btx.done {
		return nil, sql.ErrTxDone
	}
	return btx.bc.Exec(query, args...)
}

func (btx *BatchTx) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return btx.bc.QueryRowContext(ctx, query, args...)
}

func (btx *BatchTx) QueryRow(query string, args ...any) *sql.Row {
	return btx.bc.QueryRow(query, args...)
}

func (btx *BatchTx) QueryxContext(ctx context.Context, query string, args ...any) (*sqlx.Rows, error) {
	if btx.done {
		return nil, sql.ErrTxDone
	}
	return btx.bc.QueryxContext(ctx, query, args...)
}

func (btx *BatchTx) Queryx(query string, args ...any) (*sqlx.Rows, error) {
	if btx.done {
		return nil, sql.ErrTxDone
	}
	return btx.bc.Queryx(query, args...)
}

func (btx *BatchTx) QueryRowxContext(ctx context.Context, query string, args ...any) *sqlx.Row {
	return btx.bc.QueryRowxContext(ctx, query, args...)
}

func (btx *BatchTx) QueryRowx(query string, args ...any) *sqlx.Row {
	return btx.bc.QueryRowx(query, args...)
}

func (btx *BatchTx) MustExecContext(ctx context.Context, query string, args ...any) sql.Result {
	return btx.bc.MustExecContext(ctx, query, args...)
}

func (btx *BatchTx) MustExec(query string, args ...any) sql.Result {
	return btx.bc.MustExec(query, args...)
}

func (btx *BatchTx) GetContext(ctx context.Context, dest any, query string, args ...any) error {
	if btx.done {
		return sql.ErrTxDone
	}
	return btx.bc.GetContext(ctx, dest, query, args...)
}

func (btx *BatchTx) Get(dest any, query string, args ...any) error {
	if btx.done {
		return sql.ErrTxDone
	}
	return btx.bc.Get(dest, query, args...)
}

func (btx *BatchTx) SelectContext(ctx context.Context, dest any, query string, args ...any) error {
	if btx.done {
		return sql.ErrTxDone
	}
	return btx.bc.SelectContext(ctx, dest, query, args...)
}

func (btx *BatchTx) Select(dest any, query string, args ...any) error {
	if btx.done {
		return sql.ErrTxDone
	}
	return btx.bc.Select(dest, query, args...)
}

func (btx *BatchTx) NamedQueryContext(ctx context.Context, query string, arg any) (*sqlx.Rows, error) {
	if btx.done {
		return nil, sql.ErrTxDone
	}
	return btx.bc.NamedQueryContext(ctx, query, arg)
}

func (btx *BatchTx) NamedQuery(query string, arg any) (*sqlx.Rows, error) {
	if btx.done {
		return nil, sql.ErrTxDone
	}
	return btx.bc.NamedQuery(query, arg)
}

func (btx *BatchTx) NamedExecContext(ctx context.Context, query string, arg any) (sql.Result, error) {
	if btx.done {
		return nil, sql.ErrTxDone
	}
	return btx.bc.NamedExecContext(ctx, query, arg)
}

func (btx *BatchTx) NamedExec(query string, arg any) (sql.Result, error) {
	if btx.done {
		return nil, sql.ErrTxDone
	}
	return btx.bc.NamedExec(query, arg)
}

func (btx *BatchTx) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	if btx.done {
		return nil, sql.ErrTxDone
	}
	if btx.bc.isBatchRunning() {
		return nil, ErrStmtNotSupported
	}

	return btx.tx.PrepareContext(ctx, query)
}

func (btx *BatchTx) PreparexContext(ctx context.Context, query string) (*sqlx.Stmt, error) {
	if btx.done {
		return nil, sql.ErrTxDone
	}
	if btx.bc.isBatchRunning() {
		return nil, ErrStmtNotSupported
	}

	return btx.tx.PreparexContext(ctx, query)
}
