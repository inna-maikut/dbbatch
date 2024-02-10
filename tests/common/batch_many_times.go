package common

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/inna-maikut/dbbatch"
)

func BatchManyTimes(ctx context.Context, t *testing.T, db *dbbatch.BatchDB) {
	err := PrepareDB(ctx, db)
	require.NoError(t, err)

	const (
		nameFirst            = "first"
		nameSecond           = "second"
		userID         int64 = 100500
		execCount            = 100
		iterationCount       = 100
	)

	insertDefault := func(t *testing.T) {
		items := make([]Item, 0, 100)
		for i := int64(0); i < int64(execCount); i++ {
			items = append(items, Item{
				Name:   nameFirst,
				UserID: userID + i,
			})
		}
		_, err := db.NamedExec("insert into items (name, user_id) values (:name, :user_id)", items)
		require.NoError(t, err)
	}

	insertDefault(t)

	batchStartTime := time.Now()
	for j := 0; j < iterationCount; j++ {
		b := &dbbatch.Batch{}
		for i := int64(0); i < int64(execCount); i++ {
			b.Add(func(ctx context.Context) error {
				_, err := db.ExecContext(ctx, "update items set name = $1 where user_id = $2", nameSecond, userID+i)
				return err
			})
		}

		err := db.SendBatch(ctx, b)
		if err != nil {
			panic(fmt.Errorf("error in SendBatchRequests: %w", err))
		}

	}
	batchDuration := time.Since(batchStartTime)
	fmt.Printf(
		"batch total = %s, per batch (with %d execs) = %s\n",
		batchDuration,
		execCount,
		batchDuration/time.Duration(iterationCount),
	)
}
