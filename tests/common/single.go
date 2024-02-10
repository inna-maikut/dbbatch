//go:build integration

package common

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/inna-maikut/dbbatch"
)

func Single(ctx context.Context, t *testing.T, db *dbbatch.BatchDB) {
	err := PrepareDB(ctx, db)
	require.NoError(t, err)

	const (
		nameFirst        = "first"
		nameSecond       = "second"
		nameThird        = "third"
		userID     int64 = 100100
	)

	_, err = db.Exec("insert into items (name, user_id) values ($1, $2)", nameFirst, userID)
	require.NoError(t, err)

	_, err = db.ExecContext(ctx, "insert into items (name, user_id) values ($1, $2)", nameSecond, userID)
	require.NoError(t, err)

	queryAll := "select id, name, user_id, create_time from items where user_id = $1 order by id"
	queryAllNamed := "select id, name, user_id, create_time from items where user_id = :user_id order by id"
	execInsert := "insert into items (name, user_id) values ($1, $2)"
	execInsertNamed := "insert into items (name, user_id) values (:name, :user_id)"
	argsAll := []interface{}{userID}
	queryOne := "select id, name, user_id, create_time from items where user_id = $1 and name = $2 order by id"
	queryOneNamed := "select id, name, user_id, create_time from items where user_id = :user_id and name = :name order by id"
	argsOneFirst := []interface{}{userID, nameFirst}
	argsOneSecond := []interface{}{userID, nameSecond}
	argsInsert := []interface{}{nameThird, userID}

	assertAll := func(t *testing.T, items []Item) {
		assert.Len(t, items, 2)
		assert.Equal(t, items[0].Name, nameFirst)
		assert.Equal(t, items[0].UserID, userID)
		assert.Equal(t, items[1].Name, nameSecond)
	}

	assertOneFirst := func(t *testing.T, it Item) {
		assert.Equal(t, it.Name, nameFirst)
		assert.Equal(t, it.UserID, userID)
		assert.False(t, it.CreateTime.IsZero())
	}

	assertOneSecond := func(t *testing.T, it Item) {
		assert.Equal(t, it.Name, nameSecond)
		assert.Equal(t, it.UserID, userID)
		assert.False(t, it.CreateTime.IsZero())
	}
	assertInserted := func(t *testing.T) {
		var items []Item
		err = db.Select(&items, queryAll, argsAll...)
		require.NoError(t, err)

		assert.Len(t, items, 3)
		assert.Equal(t, items[2].Name, nameThird)
		assert.Equal(t, items[2].UserID, userID)

		_, err = db.Exec("delete from items where name = $1", nameThird)
		require.NoError(t, err)
	}

	scanItemFromRow := func(t *testing.T, row *sql.Row) (it Item) {
		err = row.Scan(&it.ID, &it.Name, &it.UserID, &it.CreateTime)
		require.NoError(t, err)

		return it
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

	_ = argsOneSecond
	_ = assertOneSecond
	_ = queryOneNamed

	t.Run("Query", func(t *testing.T) {
		rows, err := db.Query(queryAll, argsAll...)
		require.NoError(t, err)

		defer rows.Close()

		assertAll(t, scanItemsFromRows(t, rows))
	})

	t.Run("QueryContext", func(t *testing.T) {
		rows, err := db.QueryContext(ctx, queryAll, argsAll...)
		require.NoError(t, err)

		defer rows.Close()

		assertAll(t, scanItemsFromRows(t, rows))
	})

	t.Run("Queryx", func(t *testing.T) {
		rows, err := db.Queryx(queryAll, argsAll...)
		require.NoError(t, err)

		defer rows.Close()

		assertAll(t, scanItemsFromRowsx(t, rows))
	})

	t.Run("QueryxContext", func(t *testing.T) {
		rows, err := db.QueryxContext(ctx, queryAll, argsAll...)
		require.NoError(t, err)

		defer rows.Close()

		assertAll(t, scanItemsFromRowsx(t, rows))
	})

	t.Run("Exec", func(t *testing.T) {
		res, err := db.Exec(execInsert, argsInsert...)
		require.NoError(t, err)

		rowsAffected, err := res.RowsAffected()
		require.NoError(t, err)
		assert.Equal(t, int64(1), rowsAffected)

		assertInserted(t)
	})

	t.Run("ExecContext", func(t *testing.T) {
		res, err := db.ExecContext(ctx, execInsert, argsInsert...)
		require.NoError(t, err)

		rowsAffected, err := res.RowsAffected()
		require.NoError(t, err)
		assert.Equal(t, int64(1), rowsAffected)

		assertInserted(t)
	})

	t.Run("ExecContext_with_cancel", func(t *testing.T) {
		cancelCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		ch := make(chan struct{})
		go func() {
			tx, err := db.BeginTxx(ctx, &sql.TxOptions{})
			require.NoError(t, err)
			_, err = tx.ExecContext(ctx, "select pg_advisory_xact_lock(1001)")
			require.NoError(t, err)
			<-ch
			ch <- struct{}{}
			_ = tx.Rollback()
		}()

		ch <- struct{}{}
		time.AfterFunc(100*time.Millisecond, func() {
			<-ch
		})
		time.AfterFunc(50*time.Millisecond, func() {
			cancel()
		})
		_, err = db.ExecContext(cancelCtx, "select pg_advisory_xact_lock(1001)")
		require.True(t, errors.Is(err, context.Canceled))
	})

	t.Run("NamedQuery", func(t *testing.T) {
		rows, err := db.NamedQuery(queryAllNamed, Item{UserID: userID})
		require.NoError(t, err)

		defer rows.Close()

		assertAll(t, scanItemsFromRowsx(t, rows))
	})

	t.Run("NamedQueryContext", func(t *testing.T) {
		rows, err := db.NamedQueryContext(ctx, queryAllNamed, Item{UserID: userID})
		require.NoError(t, err)

		defer rows.Close()

		assertAll(t, scanItemsFromRowsx(t, rows))
	})

	t.Run("NamedExec", func(t *testing.T) {
		res, err := db.NamedExec(execInsertNamed, Item{UserID: userID, Name: nameThird})
		require.NoError(t, err)

		rowsAffected, err := res.RowsAffected()
		require.NoError(t, err)
		assert.Equal(t, int64(1), rowsAffected)

		assertInserted(t)
	})

	t.Run("NamedExecContext", func(t *testing.T) {
		res, err := db.NamedExecContext(ctx, execInsertNamed, Item{UserID: userID, Name: nameThird})
		require.NoError(t, err)

		rowsAffected, err := res.RowsAffected()
		require.NoError(t, err)
		assert.Equal(t, int64(1), rowsAffected)

		assertInserted(t)
	})

	t.Run("Select", func(t *testing.T) {
		var items []Item
		err = db.Select(&items, queryAll, argsAll...)
		require.NoError(t, err)

		assertAll(t, items)
	})

	t.Run("SelectContext", func(t *testing.T) {
		var items []Item
		err = db.SelectContext(ctx, &items, queryAll, argsAll...)
		require.NoError(t, err)

		assertAll(t, items)
	})

	t.Run("QueryRow", func(t *testing.T) {
		row := db.QueryRow(queryOne, argsOneFirst...)
		require.NoError(t, row.Err())

		assertOneFirst(t, scanItemFromRow(t, row))
	})

	t.Run("QueryRowContext", func(t *testing.T) {
		row := db.QueryRowContext(ctx, queryOne, argsOneFirst...)
		require.NoError(t, row.Err())

		assertOneFirst(t, scanItemFromRow(t, row))
	})

	t.Run("QueryRowx", func(t *testing.T) {
		row := db.QueryRowx(queryOne, argsOneFirst...)
		require.NoError(t, row.Err())

		assertOneFirst(t, scanItemFromRowx(t, row))
	})

	t.Run("Get", func(t *testing.T) {
		var it Item
		err = db.Get(&it, queryOne, argsOneFirst...)
		require.NoError(t, err)

		assertOneFirst(t, it)
	})

	t.Run("GetContext", func(t *testing.T) {
		var it Item
		err = db.GetContext(ctx, &it, queryOne, argsOneFirst...)
		require.NoError(t, err)

		assertOneFirst(t, it)
	})

	t.Run("Prepare", func(t *testing.T) {
		st, err := db.Prepare(queryAll)
		require.NoError(t, err)
		defer st.Close()

		rows, err := st.Query(argsAll...)
		require.NoError(t, err)

		assertAll(t, scanItemsFromRows(t, rows))
	})

	t.Run("PrepareContext", func(t *testing.T) {
		st, err := db.PrepareContext(ctx, queryAll)
		require.NoError(t, err)
		defer st.Close()

		rows, err := st.Query(argsAll...)
		require.NoError(t, err)

		assertAll(t, scanItemsFromRows(t, rows))
	})

	t.Run("Preparex", func(t *testing.T) {
		st, err := db.Preparex(queryAll)
		require.NoError(t, err)
		defer st.Close()

		rows, err := st.Queryx(argsAll...)
		require.NoError(t, err)

		assertAll(t, scanItemsFromRowsx(t, rows))
	})

	t.Run("PreparexContext", func(t *testing.T) {
		st, err := db.PreparexContext(ctx, queryAll)
		require.NoError(t, err)
		defer st.Close()

		rows, err := st.Queryx(argsAll...)
		require.NoError(t, err)

		assertAll(t, scanItemsFromRowsx(t, rows))
	})

	t.Run("PrepareNamed", func(t *testing.T) {
		stmt, err := db.PrepareNamed(queryAllNamed)
		require.NoError(t, err)
		defer stmt.Close()

		rows, err := stmt.Query(Item{UserID: userID})
		require.NoError(t, err)

		assertAll(t, scanItemsFromRows(t, rows))
	})

	t.Run("PrepareNamedContext", func(t *testing.T) {
		stmt, err := db.PrepareNamedContext(ctx, queryAllNamed)
		require.NoError(t, err)
		defer stmt.Close()

		rows, err := stmt.Query(Item{UserID: userID})
		require.NoError(t, err)

		assertAll(t, scanItemsFromRows(t, rows))
	})

	t.Run("Rebind", func(t *testing.T) {
		s := db.Rebind("select * from items where user_id = ?")
		assert.Equal(t, "select * from items where user_id = $1", s)
	})

	t.Run("TxRollback", func(t *testing.T) {
		tx, err := db.BeginTx(ctx, &sql.TxOptions{})
		require.NoError(t, err)

		_, err = tx.Exec(db.Rebind("delete from items where user_id = ?"), userID)
		require.NoError(t, err)

		err = tx.Rollback()
		require.NoError(t, err)

		var items []Item
		err = db.Select(&items, queryAll, argsAll...)
		require.NoError(t, err)

		assertAll(t, items)
	})

	t.Run("TxCommitWithMustBegin", func(t *testing.T) {
		tx := db.MustBeginTx(ctx, &sql.TxOptions{})

		_, err = tx.Exec(db.Rebind("insert into items (user_id, name) values (?, ?)"), userID+1, "third")
		require.NoError(t, err)

		err = tx.Commit()
		require.NoError(t, err)

		var items []Item
		err = db.Select(&items, queryAll, userID+1)
		require.NoError(t, err)

		_, err = db.Exec("delete from items where user_id = $1", userID+1)
		require.NoError(t, err)

		assert.Len(t, items, 1)
	})

	t.Run("TxCommit", func(t *testing.T) {
		tx, err := db.BeginTx(ctx, &sql.TxOptions{})
		require.NoError(t, err)

		_, err = tx.Exec(db.Rebind("insert into items (user_id, name) values (?, ?)"), userID+1, "third")
		require.NoError(t, err)

		err = tx.Commit()
		require.NoError(t, err)

		var items []Item
		err = db.Select(&items, queryAll, userID+1)
		require.NoError(t, err)

		_, err = db.Exec("delete from items where user_id = $1", userID+1)
		require.NoError(t, err)

		assert.Len(t, items, 1)
	})

	t.Run("TxxRollback", func(t *testing.T) {
		tx, err := db.BeginTxx(ctx, &sql.TxOptions{})
		require.NoError(t, err)

		_, err = tx.NamedExec("delete from items where user_id = :user_id", Item{UserID: userID})
		require.NoError(t, err)

		err = tx.Rollback()
		require.NoError(t, err)

		var items []Item
		err = db.Select(&items, queryAll, argsAll...)
		require.NoError(t, err)

		assertAll(t, items)
	})

	t.Run("TxxCommit", func(t *testing.T) {
		tx, err := db.BeginTxx(ctx, &sql.TxOptions{})
		require.NoError(t, err)

		_, err = tx.NamedExec(
			"insert into items (user_id, name) values (:user_id, :name)",
			Item{UserID: userID + 2, Name: "fourth"},
		)
		require.NoError(t, err)

		err = tx.Commit()
		require.NoError(t, err)

		var items []Item
		err = db.Select(&items, queryAll, userID+2)
		require.NoError(t, err)

		_, err = db.Exec("delete from items where user_id = $1", userID+2)
		require.NoError(t, err)

		assert.Len(t, items, 1)
	})
}
