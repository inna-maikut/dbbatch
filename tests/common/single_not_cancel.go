//go:build integration

package common

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/inna-maikut/dbbatch"
)

func SingleNotCancel(ctx context.Context, t *testing.T, db *dbbatch.BatchDB) {
	err := PrepareDB(ctx, db)
	require.NoError(t, err)

	const (
		nameFirst        = "first"
		nameSecond       = "second"
		userID     int64 = 100100
	)

	_, err = db.Exec("insert into items (name, user_id) values ($1, $2)", nameFirst, userID)
	require.NoError(t, err)

	_, err = db.ExecContext(ctx, "insert into items (name, user_id) values ($1, $2)", nameSecond, userID)
	require.NoError(t, err)

	t.Run("ExecContext_no_cancel", func(t *testing.T) {
		cancelCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		ch := make(chan struct{})
		go func() {
			tx, err := db.BeginTxx(ctx, &sql.TxOptions{})
			require.NoError(t, err)
			_, err = tx.ExecContext(ctx, "select pg_advisory_xact_lock(2001)")
			require.NoError(t, err)
			<-ch
			ch <- struct{}{}
			err = tx.Rollback()
			require.NoError(t, err)
		}()

		ch <- struct{}{}
		time.AfterFunc(100*time.Millisecond, func() {
			<-ch
		})
		time.AfterFunc(50*time.Millisecond, func() {
			cancel()
		})
		startTime := time.Now()
		_, err = db.ExecContext(cancelCtx, "select pg_advisory_xact_lock(2001)")
		require.NoError(t, err)

		assert.Greater(t, time.Since(startTime), 100*time.Millisecond)
	})
}
