package dbbatch

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
)

//nolint:sqlclosecheck
func TestBatchDB_withDoneConn(t *testing.T) {
	bc := &BatchConn{}
	bc.done = true

	ctx := SetBatchConnToContext(context.Background(), bc)

	bdb := &BatchDB{}

	t.Run("BeginBatchTx", func(t *testing.T) {
		_, err := bdb.BeginBatchTx(ctx, &sql.TxOptions{})
		assert.EqualError(t, err, "bdb.BatchConn: don't support nested batch")
	})

	t.Run("SendBatch", func(t *testing.T) {
		err := bdb.SendBatch(ctx, &Batch{})
		assert.ErrorIs(t, err, sql.ErrConnDone)
	})

	t.Run("QueryContext", func(t *testing.T) {
		_, err := bdb.QueryContext(ctx, "")
		assert.ErrorIs(t, err, sql.ErrConnDone)
	})

	t.Run("ExecContext", func(t *testing.T) {
		_, err := bdb.ExecContext(ctx, "")
		assert.ErrorIs(t, err, sql.ErrConnDone)
	})

	// QueryRowContext, QueryRow, QueryRowxContext, QueryRowx will return err from inner conn

	t.Run("QueryxContext", func(t *testing.T) {
		_, err := bdb.QueryxContext(ctx, "")
		assert.ErrorIs(t, err, sql.ErrConnDone)
	})

	t.Run("GetContext", func(t *testing.T) {
		var val any
		err := bdb.GetContext(ctx, val, "")
		assert.ErrorIs(t, err, sql.ErrConnDone)
	})

	t.Run("SelectContext", func(t *testing.T) {
		var val any
		err := bdb.SelectContext(ctx, val, "")
		assert.ErrorIs(t, err, sql.ErrConnDone)
	})

	t.Run("NamedQueryContext", func(t *testing.T) {
		_, err := bdb.NamedQueryContext(ctx, "", struct{}{})
		assert.ErrorIs(t, err, sql.ErrConnDone)
	})

	t.Run("NamedExecContext", func(t *testing.T) {
		_, err := bdb.NamedExecContext(ctx, "", struct{}{})
		assert.ErrorIs(t, err, sql.ErrConnDone)
	})
}

func TestBatchDB_TxErrWithConn(t *testing.T) {
	bc := &BatchConn{}
	bc.done = true

	ctx := SetBatchConnToContext(context.Background(), bc)

	bdb := &BatchDB{}

	t.Run("BeginTx", func(t *testing.T) {
		_, err := bdb.BeginTx(ctx, &sql.TxOptions{})
		assert.EqualError(t, err, "transaction is not supported in batch, use BeginBatchTx method")
	})

	t.Run("BeginTxx", func(t *testing.T) {
		_, err := bdb.BeginTxx(ctx, &sql.TxOptions{})
		assert.EqualError(t, err, "transaction is not supported in batch, use BeginBatchTx method")
	})
}
