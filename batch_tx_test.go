package dbbatch

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBatchTx_withDoneConn(t *testing.T) {
	bc := &BatchConn{}
	bc.done = true

	ctx := SetBatchConnToContext(context.Background(), bc)

	btx := &BatchTx{bc: bc}

	t.Run("SendBatch", func(t *testing.T) {
		err := btx.SendBatch(ctx, &Batch{})
		assert.ErrorIs(t, err, sql.ErrConnDone)
	})

	t.Run("QueryContext", func(t *testing.T) {
		rows, err := btx.QueryContext(ctx, "")
		if rows != nil {
			defer rows.Close()
		}
		assert.ErrorIs(t, err, sql.ErrConnDone)
	})

	t.Run("ExecContext", func(t *testing.T) {
		_, err := btx.ExecContext(ctx, "")
		assert.ErrorIs(t, err, sql.ErrConnDone)
	})

	// QueryRowContext, QueryRow, QueryRowxContext, QueryRowx will return err from inner conn

	t.Run("QueryxContext", func(t *testing.T) {
		rows, err := btx.QueryxContext(ctx, "")
		if rows != nil {
			defer rows.Close()
		}
		assert.ErrorIs(t, err, sql.ErrConnDone)
	})

	t.Run("GetContext", func(t *testing.T) {
		var val any
		err := btx.GetContext(ctx, val, "")
		assert.ErrorIs(t, err, sql.ErrConnDone)
	})

	t.Run("SelectContext", func(t *testing.T) {
		var val any
		err := btx.SelectContext(ctx, val, "")
		assert.ErrorIs(t, err, sql.ErrConnDone)
	})

	t.Run("NamedQueryContext", func(t *testing.T) {
		rows, err := btx.NamedQueryContext(ctx, "", struct{}{})
		if rows != nil {
			defer rows.Close()
		}
		assert.ErrorIs(t, err, sql.ErrConnDone)
	})

	t.Run("NamedExecContext", func(t *testing.T) {
		_, err := btx.NamedExecContext(ctx, "", struct{}{})
		assert.ErrorIs(t, err, sql.ErrConnDone)
	})
}

func TestBatchTx_DoneState(t *testing.T) {
	ctx := context.Background()

	btx := &BatchTx{}
	btx.done = true

	t.Run("Commit", func(t *testing.T) {
		err := btx.Commit()
		assert.ErrorIs(t, err, sql.ErrTxDone)
	})

	t.Run("Rollback", func(t *testing.T) {
		err := btx.Rollback()
		assert.ErrorIs(t, err, sql.ErrTxDone)
	})

	t.Run("SendBatch", func(t *testing.T) {
		err := btx.SendBatch(ctx, &Batch{})
		assert.ErrorIs(t, err, sql.ErrTxDone)
	})

	t.Run("QueryContext", func(t *testing.T) {
		rows, err := btx.QueryContext(ctx, "")
		if rows != nil {
			defer rows.Close()
		}
		assert.ErrorIs(t, err, sql.ErrTxDone)
	})

	t.Run("Query", func(t *testing.T) {
		rows, err := btx.Query("")
		if rows != nil {
			defer rows.Close()
		}
		assert.ErrorIs(t, err, sql.ErrTxDone)
	})

	t.Run("ExecContext", func(t *testing.T) {
		_, err := btx.ExecContext(ctx, "")
		assert.ErrorIs(t, err, sql.ErrTxDone)
	})

	t.Run("Exec", func(t *testing.T) {
		_, err := btx.Exec("")
		assert.ErrorIs(t, err, sql.ErrTxDone)
	})

	// QueryRowContext, QueryRow, QueryRowxContext, QueryRowx will return err from inner conn

	t.Run("QueryxContext", func(t *testing.T) {
		rows, err := btx.QueryxContext(ctx, "")
		if rows != nil {
			defer rows.Close()
		}
		assert.ErrorIs(t, err, sql.ErrTxDone)
	})

	t.Run("Queryx", func(t *testing.T) {
		rows, err := btx.Queryx("")
		if rows != nil {
			defer rows.Close()
		}
		assert.ErrorIs(t, err, sql.ErrTxDone)
	})

	t.Run("GetContext", func(t *testing.T) {
		var val any
		err := btx.GetContext(ctx, val, "")
		assert.ErrorIs(t, err, sql.ErrTxDone)
	})

	t.Run("Get", func(t *testing.T) {
		var val any
		err := btx.Get(val, "")
		assert.ErrorIs(t, err, sql.ErrTxDone)
	})

	t.Run("SelectContext", func(t *testing.T) {
		var val any
		err := btx.SelectContext(ctx, val, "")
		assert.ErrorIs(t, err, sql.ErrTxDone)
	})

	t.Run("Select", func(t *testing.T) {
		var val any
		err := btx.Select(val, "")
		assert.ErrorIs(t, err, sql.ErrTxDone)
	})

	t.Run("NamedQueryContext", func(t *testing.T) {
		rows, err := btx.NamedQueryContext(ctx, "", struct{}{})
		if rows != nil {
			defer rows.Close()
		}
		assert.ErrorIs(t, err, sql.ErrTxDone)
	})

	t.Run("NamedQuery", func(t *testing.T) {
		rows, err := btx.NamedQuery("", struct{}{})
		if rows != nil {
			defer rows.Close()
		}
		assert.ErrorIs(t, err, sql.ErrTxDone)
	})

	t.Run("NamedExecContext", func(t *testing.T) {
		_, err := btx.NamedExecContext(ctx, "", struct{}{})
		assert.ErrorIs(t, err, sql.ErrTxDone)
	})

	t.Run("NamedExec", func(t *testing.T) {
		_, err := btx.NamedExec("", struct{}{})
		assert.ErrorIs(t, err, sql.ErrTxDone)
	})
}
