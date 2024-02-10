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

func BatchOneStep(ctx context.Context, t *testing.T, db *dbbatch.BatchDB) {
	err := PrepareDB(ctx, db)
	require.NoError(t, err)

	const (
		nameFirst        = "first"
		nameSecond       = "second"
		nameThird        = "third"
		userID     int64 = 100200
	)

	insertDefault := func(t *testing.T, i int64) {
		_, err := db.NamedExec("insert into items (name, user_id) values (:name, :user_id)", []Item{
			{Name: nameFirst, UserID: userID + i},
			{Name: nameSecond, UserID: userID + i},
		})
		require.NoError(t, err)
	}

	queryAll := "select id, name, user_id, create_time from items where user_id = $1 order by id"
	queryAllNamed := "select id, name, user_id, create_time from items where user_id = :user_id order by id"
	execInsert := "insert into items (name, user_id) values ($1, $2)"
	execInsertNamed := "insert into items (name, user_id) values (:name, :user_id)"
	queryOne := "select id, name, user_id, create_time from items where user_id = $1 and name = $2 order by id"

	assertAll := func(t *testing.T, items []Item, i int64) {
		t.Helper()
		assert.Len(t, items, 2)
		assert.Equal(t, items[0].Name, nameFirst)
		assert.Equal(t, items[0].UserID, userID+i)
		assert.Equal(t, items[1].Name, nameSecond)
		assert.Equal(t, items[1].UserID, userID+i)
	}

	assertOneFirst := func(t *testing.T, it Item, i int64) {
		t.Helper()
		assert.Equal(t, it.Name, nameFirst)
		assert.Equal(t, it.UserID, userID+i)
		assert.False(t, it.CreateTime.IsZero())
	}

	assertOneSecond := func(t *testing.T, it Item, i int64) {
		t.Helper()
		assert.Equal(t, it.Name, nameSecond)
		assert.Equal(t, it.UserID, userID+i)
		assert.False(t, it.CreateTime.IsZero())
	}
	_ = assertOneSecond

	assertInserted := func(t *testing.T, i int64) {
		var items []Item
		err := db.SelectContext(ctx, &items, queryAll, userID+i)
		require.NoError(t, err)

		assert.Len(t, items, 1)
		assert.Equal(t, items[0].Name, nameThird)
		assert.Equal(t, items[0].UserID, userID+i)

		_, err = db.Exec("delete from items where user_id = $1", userID+i)
		require.NoError(t, err)
	}

	scanItemFromRow := func(t *testing.T, row *sql.Row) (it Item) {
		err := row.Scan(&it.ID, &it.Name, &it.UserID, &it.CreateTime)
		require.NoError(t, err)

		return it
	}

	scanItemFromRowx := func(t *testing.T, row *sqlx.Row) (it Item) {
		err := row.StructScan(&it)
		require.NoError(t, err)

		return it
	}

	scanItemsFromRows := func(t *testing.T, rows *sql.Rows) (items []Item) {
		for rows.Next() {
			it := Item{}
			err := rows.Scan(&it.ID, &it.Name, &it.UserID, &it.CreateTime)
			require.NoError(t, err)
			items = append(items, it)
		}

		return items
	}

	scanItemsFromRowsx := func(t *testing.T, rows *sqlx.Rows) (items []Item) {
		for rows.Next() {
			it := Item{}
			err := rows.StructScan(&it)
			require.NoError(t, err)
			items = append(items, it)
		}

		return items
	}

	_ = queryAllNamed
	_ = execInsert
	_ = execInsertNamed
	_ = queryOne
	_ = assertOneFirst
	_ = assertInserted
	_ = scanItemFromRow
	_ = scanItemFromRowx
	_ = scanItemsFromRows
	_ = scanItemsFromRowsx

	i := int64(0)

	b := &dbbatch.Batch{}

	b.Add(func(ctx context.Context) error {
		i++
		i := i
		insertDefault(t, i)

		rows, err := db.QueryContext(ctx, queryAll, userID+i)
		require.NoError(t, err)

		defer rows.Close()

		assertAll(t, scanItemsFromRows(t, rows), i)

		return nil
	})

	b.Add(func(ctx context.Context) error {
		i++
		i := i
		insertDefault(t, i)

		rows, err := db.QueryxContext(ctx, queryAll, userID+i)
		require.NoError(t, err)

		defer rows.Close()

		assertAll(t, scanItemsFromRowsx(t, rows), i)

		return nil
	})

	b.Add(func(ctx context.Context) error {
		i++
		i := i
		res, err := db.Exec(execInsert, nameThird, userID+i)
		require.NoError(t, err)

		rowsAffected, err := res.RowsAffected()
		require.NoError(t, err)
		assert.Equal(t, int64(1), rowsAffected)

		assertInserted(t, i)

		return nil
	})

	b.Add(func(ctx context.Context) error {
		i++
		i := i
		res, err := db.ExecContext(ctx, execInsert, nameThird, userID+i)
		require.NoError(t, err)

		rowsAffected, err := res.RowsAffected()
		require.NoError(t, err)
		assert.Equal(t, int64(1), rowsAffected)

		assertInserted(t, i)

		return nil
	})

	b.Add(func(ctx context.Context) error {
		i++
		i := i
		res := db.MustExecContext(ctx, execInsert, nameThird, userID+i)

		rowsAffected, err := res.RowsAffected()
		require.NoError(t, err)
		assert.Equal(t, int64(1), rowsAffected)

		assertInserted(t, i)

		return nil
	})

	b.Add(func(ctx context.Context) error {
		i++
		i := i
		res, err := db.NamedExec(execInsertNamed, Item{UserID: userID + i, Name: nameThird})
		require.NoError(t, err)

		rowsAffected, err := res.RowsAffected()
		require.NoError(t, err)
		assert.Equal(t, int64(1), rowsAffected)

		assertInserted(t, i)

		return nil
	})

	b.Add(func(ctx context.Context) error {
		i++
		i := i
		res, err := db.NamedExecContext(ctx, execInsertNamed, Item{UserID: userID + i, Name: nameThird})
		require.NoError(t, err)

		rowsAffected, err := res.RowsAffected()
		require.NoError(t, err)
		assert.Equal(t, int64(1), rowsAffected)

		assertInserted(t, i)

		return nil
	})

	b.Add(func(ctx context.Context) error {
		i++
		i := i
		insertDefault(t, i)

		rows, err := db.NamedQueryContext(ctx, queryAllNamed, Item{UserID: userID + i})
		require.NoError(t, err)

		defer rows.Close()

		assertAll(t, scanItemsFromRowsx(t, rows), i)

		return nil
	})

	b.Add(func(ctx context.Context) error {
		i++
		i := i
		insertDefault(t, i)

		var items []Item
		err := db.SelectContext(ctx, &items, queryAll, userID+i)
		require.NoError(t, err)

		assertAll(t, items, i)

		return nil
	})

	b.Add(func(ctx context.Context) error {
		i++
		i := i
		insertDefault(t, i)

		row := db.QueryRowContext(ctx, queryOne, userID+i, nameFirst)
		require.NoError(t, row.Err())

		assertOneFirst(t, scanItemFromRow(t, row), i)

		return nil
	})

	b.Add(func(ctx context.Context) error {
		i++
		i := i
		insertDefault(t, i)

		row := db.QueryRowxContext(ctx, queryOne, userID+i, nameFirst)
		require.NoError(t, row.Err())

		assertOneFirst(t, scanItemFromRowx(t, row), i)

		return nil
	})

	b.Add(func(ctx context.Context) error {
		i++
		i := i
		insertDefault(t, i)

		var it Item
		err := db.GetContext(ctx, &it, queryOne, userID+i, nameFirst)
		require.NoError(t, err)

		assertOneFirst(t, it, i)

		return nil
	})

	err = db.SendBatch(ctx, b)
	require.NoError(t, err)
}
