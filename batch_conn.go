package dbbatch

import (
	"context"
	"database/sql"
	"errors"

	"github.com/jmoiron/sqlx"
)

type BatchConn struct {
	db        *BatchDB
	ext       Ext
	conn      *sqlx.Conn
	tx        *sqlx.Tx
	br        batchRunnerMachine
	bindNamed func(query string, arg interface{}) (string, []interface{}, error)
	done      bool
}

var _ Ext = &sqlx.Conn{}
var _ Ext = &sqlx.Tx{}

// newBatchConn creates *BatchConn. Must call BatchConn.Close() in the end if err is nil
func newBatchConn(db *BatchDB, conn *sqlx.Conn) *BatchConn {
	bc := &BatchConn{
		db:        db,
		ext:       conn,
		conn:      conn,
		tx:        nil,
		br:        nil,
		bindNamed: db.DB.BindNamed,
		done:      false,
	}

	return bc
}

func (bc *BatchConn) setInCtx(ctx context.Context) context.Context {
	return SetBatchConnToContext(ctx, bc)
}

func (bc *BatchConn) maybeWithoutCancel(ctx context.Context) context.Context {
	if bc.tx == nil {
		// bc.db used, it will use context without cancel
		return ctx
	}
	return bc.db.maybeWithoutCancel(ctx)
}

// SendBatchRequests returns batch result of concrete type. Must close it in the end
func (bc *BatchConn) SendBatchRequests(ctx context.Context, requests []Request) (res interface{}, closeFn func() error, err error) {
	if bc.done {
		return nil, nil, sql.ErrConnDone
	}
	err = bc.conn.Raw(func(driverConn interface{}) error {
		val, ok := driverConn.(BaseConnProvider)
		if !ok {
			return nil
		}

		baseConn := val.BaseConn()

		if batchSenderConn, ok := baseConn.(BatchRequestsSender); ok {
			res, closeFn, err = batchSenderConn.SendBatchRequests(ctx, requests)
		}

		return err
	})
	if err != nil {
		return nil, nil, err
	}
	if res == nil {
		return nil, nil, errors.New("batch sending is unsupported by driver")
	}

	return res, closeFn, nil
}

// BatchRunner Only for using in the driver implementation code!
func (bc *BatchConn) BatchRunner() BatchRunner {
	return bc.br
}

// BeginBatchTx begins a transaction and allows to send all batch in one transaction.
// Don't use BatchConn anymore, only BatchTx!
// During commit or rollback BatchConn will be automatically closed.
func (bc *BatchConn) BeginBatchTx(ctx context.Context, opts *sql.TxOptions) (*BatchTx, error) {
	if bc.done {
		return nil, sql.ErrConnDone
	}
	if bc.tx != nil {
		return nil, errors.New("tx already started")
	}
	if bc.br != nil {
		return nil, ErrHasRunningBatch
	}

	tx, err := bc.conn.BeginTxx(ctx, opts)
	if err != nil {
		return nil, err
	}

	bc.ext = tx
	bc.tx = tx

	return newBatchTx(bc, tx), nil
}

func (bc *BatchConn) finishTx() {
	bc.tx = nil
}

func (bc *BatchConn) isBatchRunning() bool {
	return bc.br != nil
}

func (bc *BatchConn) Close() error {
	if bc.done {
		return sql.ErrConnDone
	}
	if bc.br != nil {
		return ErrHasRunningBatch
	}
	bc.done = true
	return bc.conn.Close()
}

func (bc *BatchConn) SendBatch(ctx context.Context, b *Batch) (err error) {
	if bc.done {
		return sql.ErrConnDone
	}
	if bc.br != nil {
		return ErrHasRunningBatch
	}
	bc.br = newBatchRunner(bc)
	ctx = bc.setInCtx(ctx)
	err = bc.br.run(ctx, b)
	bc.br = nil

	return err
}

func (bc *BatchConn) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	if bc.done {
		return nil, sql.ErrConnDone
	}
	if bc.br == nil {
		return bc.ext.QueryContext(bc.maybeWithoutCancel(ctx), query, args...)
	}
	ctx = bc.setInCtx(ctx)

	_, _ = bc.ext.QueryContext(ctx, query, args...)

	bc.br.roundTrip()

	return bc.ext.QueryContext(ctx, query, args...)
}

func (bc *BatchConn) Query(query string, args ...any) (*sql.Rows, error) {
	return bc.QueryContext(context.Background(), query, args...)
}

func (bc *BatchConn) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	if bc.done {
		return nil, sql.ErrConnDone
	}
	if bc.br == nil {
		return bc.ext.ExecContext(bc.maybeWithoutCancel(ctx), query, args...)
	}
	ctx = bc.setInCtx(ctx)

	_, _ = bc.ext.ExecContext(ctx, query, args...)

	bc.br.roundTrip()

	return bc.ext.ExecContext(ctx, query, args...)
}

func (bc *BatchConn) Exec(query string, args ...any) (sql.Result, error) {
	return bc.ExecContext(context.Background(), query, args...)
}

func (bc *BatchConn) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	if bc.br == nil {
		return bc.ext.QueryRowContext(bc.maybeWithoutCancel(ctx), query, args...)
	}
	ctx = bc.setInCtx(ctx)

	_ = bc.ext.QueryRowContext(ctx, query, args...)

	bc.br.roundTrip()

	return bc.ext.QueryRowContext(ctx, query, args...)
}

func (bc *BatchConn) QueryRow(query string, args ...any) *sql.Row {
	return bc.QueryRowContext(context.Background(), query, args...)
}

func (bc *BatchConn) QueryxContext(ctx context.Context, query string, args ...any) (*sqlx.Rows, error) {
	if bc.done {
		return nil, sql.ErrConnDone
	}
	if bc.br == nil {
		return bc.ext.QueryxContext(bc.maybeWithoutCancel(ctx), query, args...)
	}
	ctx = bc.setInCtx(ctx)

	_, _ = bc.ext.QueryxContext(ctx, query, args...)

	bc.br.roundTrip()

	return bc.ext.QueryxContext(ctx, query, args...)
}

func (bc *BatchConn) Queryx(query string, args ...any) (*sqlx.Rows, error) {
	return bc.QueryxContext(context.Background(), query, args...)
}

func (bc *BatchConn) QueryRowxContext(ctx context.Context, query string, args ...any) *sqlx.Row {
	if bc.br == nil {
		return bc.ext.QueryRowxContext(bc.maybeWithoutCancel(ctx), query, args...)
	}
	ctx = bc.setInCtx(ctx)

	_ = bc.ext.QueryRowxContext(ctx, query, args...)

	bc.br.roundTrip()

	return bc.ext.QueryRowxContext(ctx, query, args...)
}

func (bc *BatchConn) QueryRowx(query string, args ...any) *sqlx.Row {
	return bc.QueryRowxContext(context.Background(), query, args...)
}

func (bc *BatchConn) MustExecContext(ctx context.Context, query string, args ...interface{}) sql.Result {
	return sqlx.MustExecContext(ctx, bc, query, args...)
}

func (bc *BatchConn) MustExec(query string, args ...interface{}) sql.Result {
	return bc.MustExecContext(context.Background(), query, args...)
}

func (bc *BatchConn) GetContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	if bc.done {
		return sql.ErrConnDone
	}
	return sqlx.GetContext(ctx, bc, dest, query, args...)
}

func (bc *BatchConn) Get(dest interface{}, query string, args ...interface{}) error {
	if bc.done {
		return sql.ErrConnDone
	}
	return bc.GetContext(context.Background(), dest, query, args...)
}

func (bc *BatchConn) SelectContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	if bc.done {
		return sql.ErrConnDone
	}
	return sqlx.SelectContext(ctx, bc, dest, query, args...)
}

func (bc *BatchConn) Select(dest interface{}, query string, args ...interface{}) error {
	if bc.done {
		return sql.ErrConnDone
	}
	return bc.SelectContext(context.Background(), dest, query, args...)
}

func (bc *BatchConn) NamedQueryContext(ctx context.Context, query string, arg interface{}) (*sqlx.Rows, error) {
	if bc.done {
		return nil, sql.ErrConnDone
	}
	q, args, err := bc.bindNamed(query, arg)
	if err != nil {
		return nil, err
	}
	return bc.QueryxContext(ctx, q, args...)
}

func (bc *BatchConn) NamedQuery(query string, arg interface{}) (*sqlx.Rows, error) {
	if bc.done {
		return nil, sql.ErrConnDone
	}
	return bc.NamedQueryContext(context.Background(), query, arg)
}

func (bc *BatchConn) NamedExecContext(ctx context.Context, query string, arg interface{}) (sql.Result, error) {
	if bc.done {
		return nil, sql.ErrConnDone
	}
	q, args, err := bc.bindNamed(query, arg)
	if err != nil {
		return nil, err
	}
	return bc.ExecContext(ctx, q, args...)
}

func (bc *BatchConn) NamedExec(query string, arg interface{}) (sql.Result, error) {
	if bc.done {
		return nil, sql.ErrConnDone
	}
	return bc.NamedExecContext(context.Background(), query, arg)
}

func (bc *BatchConn) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	if bc.done {
		return nil, sql.ErrConnDone
	}
	if bc.br != nil {
		return nil, ErrTxNotSupported
	}
	if bc.tx != nil {
		return nil, ErrNestedTxNotSupported
	}

	return bc.conn.BeginTx(ctx, opts)
}

func (bc *BatchConn) BeginTxx(ctx context.Context, opts *sql.TxOptions) (*sqlx.Tx, error) {
	if bc.done {
		return nil, sql.ErrConnDone
	}
	if bc.br != nil {
		return nil, ErrTxNotSupported
	}
	if bc.tx != nil {
		return nil, ErrNestedTxNotSupported
	}

	return bc.conn.BeginTxx(ctx, opts)
}

func (bc *BatchConn) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	if bc.done {
		return nil, sql.ErrConnDone
	}
	if bc.br != nil {
		return nil, ErrStmtNotSupported
	}

	return bc.ext.PrepareContext(ctx, query)
}

func (bc *BatchConn) PreparexContext(ctx context.Context, query string) (*sqlx.Stmt, error) {
	if bc.done {
		return nil, sql.ErrConnDone
	}
	if bc.br != nil {
		return nil, ErrStmtNotSupported
	}

	return bc.ext.PreparexContext(ctx, query)
}
