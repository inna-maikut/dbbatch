//go:build integration

package common

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/inna-maikut/dbbatch"
)

func BatchTx(ctx context.Context, t *testing.T, db *dbbatch.BatchDB) {
	err := PrepareDB(ctx, db)
	require.NoError(t, err)

	const (
		nameFirst       = "first"
		userID    int64 = 100400
	)

	queryAll := "select id, name, user_id, create_time from items where user_id = $1 order by id"
	execInsert := "insert into items (name, user_id) values ($1, $2)"

	b := &dbbatch.Batch{}

	b.Add(func(ctx context.Context) error {
		res, err := db.ExecContext(ctx, execInsert, nameFirst, userID)
		require.NoError(t, err)

		rowsAffected, err := res.RowsAffected()
		require.NoError(t, err)
		assert.Equal(t, int64(1), rowsAffected)

		var items []Item

		err = db.SelectContext(ctx, &items, queryAll, userID)
		require.NoError(t, err)

		assert.Len(t, items, 1)
		assert.Equal(t, items[0].Name, nameFirst)
		assert.Equal(t, items[0].UserID, userID)

		// no ctx provided. So we check database outside batch and transaction
		err = db.Select(&items, queryAll, userID)
		require.NoError(t, err)

		assert.Len(t, items, 0)

		return nil
	})

	b.Add(func(ctx context.Context) error {
		return nil
	})

	t.Run("tx_rollback", func(t *testing.T) {
		btx, err := db.BeginBatchTx(ctx, &sql.TxOptions{})
		require.NoError(t, err)
		err = btx.SendBatch(ctx, b)
		require.NoError(t, err)
		err = btx.Rollback()
		require.NoError(t, err)
	})

	t.Run("tx_commit", func(t *testing.T) {
		btx, err := db.BeginBatchTx(ctx, &sql.TxOptions{})
		require.NoError(t, err)
		err = btx.SendBatch(ctx, b)
		require.NoError(t, err)
		err = btx.Commit()
		require.NoError(t, err)

		var items []Item

		err = db.SelectContext(ctx, &items, queryAll, userID)
		require.NoError(t, err)

		assert.Len(t, items, 1)
		assert.Equal(t, items[0].Name, nameFirst)
		assert.Equal(t, items[0].UserID, userID)
	})
}
