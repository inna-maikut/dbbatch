//go:build integration

package common

import (
	"context"
	"database/sql"
	"testing"

	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/inna-maikut/dbbatch"
)

func BatchMultiStep(ctx context.Context, t *testing.T, db *dbbatch.BatchDB) {
	err := PrepareDB(ctx, db)
	require.NoError(t, err)

	const (
		nameFirst        = "first"
		nameSecond       = "second"
		userID     int64 = 100300
	)

	_, err = db.Exec("insert into items (name, user_id) values ($1, $2)", nameFirst, userID)
	require.NoError(t, err)

	_, err = db.ExecContext(ctx, "insert into items (name, user_id) values ($1, $2)", nameSecond, userID)
	require.NoError(t, err)

	queryAll := "select id, name, user_id, create_time from items where user_id = $1 order by id"
	argsAll := []interface{}{userID}
	queryOne := "select id, name, user_id, create_time from items where user_id = $1 and name = $2 order by id"
	argsOneFirst := []interface{}{userID, nameFirst}

	assertAll := func(t *testing.T, items []Item) {
		t.Helper()
		assert.Len(t, items, 2)
		assert.Equal(t, items[0].Name, nameFirst)
		assert.Equal(t, items[0].UserID, userID)
		assert.Equal(t, items[1].Name, nameSecond)
	}

	assertOneFirst := func(t *testing.T, it Item) {
		t.Helper()
		assert.Equal(t, it.Name, nameFirst)
		assert.Equal(t, it.UserID, userID)
		assert.False(t, it.CreateTime.IsZero())
	}

	scanItemFromRowx := func(t *testing.T, row *sqlx.Row) (it Item) {
		err = row.StructScan(&it)
		require.NoError(t, err)

		return it
	}

	scanItemsFromRows := func(t *testing.T, rows *sql.Rows) (items []Item) {
		for rows.Next() {
			it := Item{}
			err = rows.Scan(&it.ID, &it.Name, &it.UserID, &it.CreateTime)
			require.NoError(t, err)
			items = append(items, it)
		}

		return items
	}

	scanItemsFromRowsx := func(t *testing.T, rows *sqlx.Rows) (items []Item) {
		for rows.Next() {
			it := Item{}
			err = rows.StructScan(&it)
			require.NoError(t, err)
			items = append(items, it)
		}

		return items
	}

	b := &dbbatch.Batch{}

	b.Add(func(ctx context.Context) error {
		rows, err := db.QueryContext(ctx, queryAll, argsAll...)
		require.NoError(t, err)

		defer rows.Close()

		assertAll(t, scanItemsFromRows(t, rows))

		var items []Item
		err = db.SelectContext(ctx, &items, queryAll, argsAll...)
		require.NoError(t, err)

		assertAll(t, items)

		return nil
	})

	b.Add(func(ctx context.Context) error {
		rows, err := db.QueryxContext(ctx, queryAll, argsAll...)
		require.NoError(t, err)

		defer rows.Close()

		assertAll(t, scanItemsFromRowsx(t, rows))

		row := db.QueryRowxContext(ctx, queryOne, argsOneFirst...)
		require.NoError(t, row.Err())

		assertOneFirst(t, scanItemFromRowx(t, row))

		return nil
	})

	b.Add(func(ctx context.Context) error {
		return nil
	})

	t.Run("without_tx", func(t *testing.T) {
		err = db.SendBatch(ctx, b)
		require.NoError(t, err)
	})

	t.Run("tx_rollback", func(t *testing.T) {
		btx, err := db.BeginBatchTx(ctx, &sql.TxOptions{})
		require.NoError(t, err)
		err = btx.SendBatch(ctx, b)
		require.NoError(t, err)
		err = btx.Rollback()
		require.NoError(t, err)
	})

	t.Run("tx_commmit", func(t *testing.T) {
		btx, err := db.BeginBatchTx(ctx, &sql.TxOptions{})
		require.NoError(t, err)
		err = btx.SendBatch(ctx, b)
		require.NoError(t, err)
		err = btx.Commit()
		require.NoError(t, err)
	})
}
