package dbbatch

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"testing"

	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestBatchConn_QueryContext(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctx := context.Background()

	extMock := NewMockExt(ctrl)
	brMock := NewMockbatchRunnerMachine(ctrl)

	bc := &BatchConn{
		ext: extMock,
		br:  brMock,
	}
	wantContext := SetBatchConnToContext(ctx, bc)
	wantRows := &sql.Rows{}

	extMock.EXPECT().QueryContext(wantContext, "query", 1, 2).Return(nil, errors.New("some error"))
	extMock.EXPECT().QueryContext(wantContext, "query", 1, 2).Return(wantRows, nil)
	brMock.EXPECT().roundTrip()

	rows, err := bc.QueryContext(ctx, "query", 1, 2)
	require.NoError(t, err)
	assert.Same(t, wantRows, rows)
}

func TestBatchConn_ExecContext(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctx := context.Background()

	extMock := NewMockExt(ctrl)
	brMock := NewMockbatchRunnerMachine(ctrl)

	bc := &BatchConn{
		ext: extMock,
		br:  brMock,
	}
	wantContext := SetBatchConnToContext(ctx, bc)
	wantRows := driver.RowsAffected(123)

	extMock.EXPECT().ExecContext(wantContext, "exec", 1, 2).Return(nil, errors.New("some error"))
	extMock.EXPECT().ExecContext(wantContext, "exec", 1, 2).Return(&wantRows, nil)
	brMock.EXPECT().roundTrip()

	rows, err := bc.ExecContext(ctx, "exec", 1, 2)
	require.NoError(t, err)
	assert.Same(t, &wantRows, rows)
}

func TestBatchConn_QueryRowContext(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctx := context.Background()

	extMock := NewMockExt(ctrl)
	brMock := NewMockbatchRunnerMachine(ctrl)

	bc := &BatchConn{
		ext: extMock,
		br:  brMock,
	}
	wantContext := SetBatchConnToContext(ctx, bc)
	wantRow := &sql.Row{}

	extMock.EXPECT().QueryRowContext(wantContext, "query", 1, 2).Return(nil)
	extMock.EXPECT().QueryRowContext(wantContext, "query", 1, 2).Return(wantRow)
	brMock.EXPECT().roundTrip()

	row := bc.QueryRowContext(ctx, "query", 1, 2)
	assert.Same(t, wantRow, row)
}

func TestBatchConn_QueryxContext(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctx := context.Background()

	extMock := NewMockExt(ctrl)
	brMock := NewMockbatchRunnerMachine(ctrl)

	bc := &BatchConn{
		ext: extMock,
		br:  brMock,
	}
	wantContext := SetBatchConnToContext(ctx, bc)
	wantRows := &sqlx.Rows{}

	extMock.EXPECT().QueryxContext(wantContext, "query", 1, 2).Return(nil, errors.New("some error"))
	extMock.EXPECT().QueryxContext(wantContext, "query", 1, 2).Return(wantRows, nil)
	brMock.EXPECT().roundTrip()

	rows, err := bc.QueryxContext(ctx, "query", 1, 2)
	require.NoError(t, err)
	assert.Same(t, wantRows, rows)
}

func TestBatchConn_QueryRowxContext(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctx := context.Background()

	extMock := NewMockExt(ctrl)
	brMock := NewMockbatchRunnerMachine(ctrl)

	bc := &BatchConn{
		ext: extMock,
		br:  brMock,
	}
	wantContext := SetBatchConnToContext(ctx, bc)
	wantRow := &sqlx.Row{}

	extMock.EXPECT().QueryRowxContext(wantContext, "query", 1, 2).Return(nil)
	extMock.EXPECT().QueryRowxContext(wantContext, "query", 1, 2).Return(wantRow)
	brMock.EXPECT().roundTrip()

	row := bc.QueryRowxContext(ctx, "query", 1, 2)
	assert.Same(t, wantRow, row)
}

func TestBatchConn_DoneState(t *testing.T) {
	ctx := context.Background()

	bc := &BatchConn{}
	bc.done = true

	t.Run("SendBatchRequests", func(t *testing.T) {
		_, _, err := bc.SendBatchRequests(ctx, []Request{})
		assert.ErrorIs(t, err, sql.ErrConnDone)
	})

	t.Run("BeginBatchTx", func(t *testing.T) {
		_, err := bc.BeginBatchTx(ctx, &sql.TxOptions{})
		assert.ErrorIs(t, err, sql.ErrConnDone)
	})

	t.Run("Close", func(t *testing.T) {
		err := bc.Close()
		assert.ErrorIs(t, err, sql.ErrConnDone)
	})

	t.Run("SendBatch", func(t *testing.T) {
		err := bc.SendBatch(ctx, &Batch{})
		assert.ErrorIs(t, err, sql.ErrConnDone)
	})

	t.Run("QueryContext", func(t *testing.T) {
		_, err := bc.QueryContext(ctx, "")
		assert.ErrorIs(t, err, sql.ErrConnDone)
	})

	t.Run("Query", func(t *testing.T) {
		_, err := bc.Query("")
		assert.ErrorIs(t, err, sql.ErrConnDone)
	})

	t.Run("ExecContext", func(t *testing.T) {
		_, err := bc.ExecContext(ctx, "")
		assert.ErrorIs(t, err, sql.ErrConnDone)
	})

	t.Run("Exec", func(t *testing.T) {
		_, err := bc.Exec("")
		assert.ErrorIs(t, err, sql.ErrConnDone)
	})

	// QueryRowContext, QueryRow, QueryRowxContext, QueryRowx will return err from inner conn

	t.Run("QueryxContext", func(t *testing.T) {
		_, err := bc.QueryxContext(ctx, "")
		assert.ErrorIs(t, err, sql.ErrConnDone)
	})

	t.Run("Queryx", func(t *testing.T) {
		_, err := bc.Queryx("")
		assert.ErrorIs(t, err, sql.ErrConnDone)
	})

	t.Run("GetContext", func(t *testing.T) {
		var val interface{}
		err := bc.GetContext(ctx, val, "")
		assert.ErrorIs(t, err, sql.ErrConnDone)
	})

	t.Run("Get", func(t *testing.T) {
		var val interface{}
		err := bc.Get(val, "")
		assert.ErrorIs(t, err, sql.ErrConnDone)
	})

	t.Run("SelectContext", func(t *testing.T) {
		var val interface{}
		err := bc.SelectContext(ctx, val, "")
		assert.ErrorIs(t, err, sql.ErrConnDone)
	})

	t.Run("Select", func(t *testing.T) {
		var val interface{}
		err := bc.Select(val, "")
		assert.ErrorIs(t, err, sql.ErrConnDone)
	})

	t.Run("NamedQueryContext", func(t *testing.T) {
		_, err := bc.NamedQueryContext(ctx, "", struct{}{})
		assert.ErrorIs(t, err, sql.ErrConnDone)
	})

	t.Run("NamedQuery", func(t *testing.T) {
		_, err := bc.NamedQuery("", struct{}{})
		assert.ErrorIs(t, err, sql.ErrConnDone)
	})

	t.Run("NamedExecContext", func(t *testing.T) {
		_, err := bc.NamedExecContext(ctx, "", struct{}{})
		assert.ErrorIs(t, err, sql.ErrConnDone)
	})

	t.Run("NamedExec", func(t *testing.T) {
		_, err := bc.NamedExec("", struct{}{})
		assert.ErrorIs(t, err, sql.ErrConnDone)
	})
}
