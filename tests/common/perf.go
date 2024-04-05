package common

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/inna-maikut/dbbatch"
)

type PerfResult struct {
	Seq           time.Duration
	SeqSingleConn time.Duration
	Batch         time.Duration
	BatchSeq      time.Duration
	Upsert        time.Duration
	SeqBatchRate  float32
}

func PerfToResult(ctx context.Context, db *dbbatch.BatchDB, iterationCount, execCount int) (res PerfResult, err error) {
	const (
		nameFirst       = "first"
		userID    int64 = 100500
	)

	// insert initial values
	items := make([]Item, 0, 100)
	for i := int64(0); i < int64(execCount); i++ {
		items = append(items, Item{
			Name:   nameFirst,
			UserID: userID + i,
		})
	}
	_, err = db.Exec("delete from items where true")
	if err != nil {
		return res, fmt.Errorf("db.Exec: %w", err)
	}
	_, err = db.NamedExec("insert into items (name, user_id) values (:name, :user_id)", items)
	if err != nil {
		return res, fmt.Errorf("db.NamedExec: %w", err)
	}

	var seqDuration, seqSingleConnDuration, batchDuration, batchSeqDuration, upsertDuration time.Duration

	for j := 0; j < iterationCount; j++ {
		if err = measureBatch(ctx, db, &batchDuration, execCount, userID); err != nil {
			return res, fmt.Errorf("measureBatch: %w", err)
		}

		if err = measureBatchSeq(ctx, db, &batchSeqDuration, execCount, userID); err != nil {
			return res, fmt.Errorf("measureBatchSeq: %w", err)
		}

		if err = measureSeq(ctx, db, &seqDuration, execCount, userID); err != nil {
			return res, fmt.Errorf("measureBatchSeq: %w", err)
		}

		if err = measureSeqSingleConn(ctx, db, &seqSingleConnDuration, execCount, userID); err != nil {
			return res, fmt.Errorf("measureBatchSeq: %w", err)
		}

		if err = measureUpsert(ctx, db, &upsertDuration, execCount, userID); err != nil {
			return res, fmt.Errorf("measureBatchSeq: %w", err)
		}
	}

	ops := time.Duration(execCount * iterationCount)

	return PerfResult{
		Seq:           seqDuration / ops,
		SeqSingleConn: seqSingleConnDuration / ops,
		Batch:         batchDuration / ops,
		BatchSeq:      batchSeqDuration / ops,
		Upsert:        upsertDuration / ops,
		SeqBatchRate:  float32(batchDuration) / float32(seqDuration),
	}, nil
}

func measureBatch(ctx context.Context, db *dbbatch.BatchDB, duration *time.Duration, execCount int, userID int64) (err error) {
	const name = "second batch"

	batchStartTime := time.Now()
	b := &dbbatch.Batch{}
	for i := int64(0); i < int64(execCount); i++ {
		i := i
		b.Add(func(ctx context.Context) error {
			_, err = db.ExecContext(ctx, "update items set name = $1 where user_id = $2", name, userID+i)
			return err
		})
	}

	err = db.SendBatch(ctx, b)
	if err != nil {
		return fmt.Errorf("batch SendBatchRequests: %w", err)
	}
	*duration += time.Since(batchStartTime)

	return nil
}

func measureBatchSeq(ctx context.Context, db *dbbatch.BatchDB, duration *time.Duration, execCount int, userID int64) (err error) {
	const name = "second batch seq"

	batchSeqStartTime := time.Now()
	b := &dbbatch.Batch{}
	for i := int64(0); i < int64(execCount); i++ {
		i := i
		b.Add(func(ctx context.Context) error {
			_, err = db.ExecContext(ctx, "update items set name = $1 where user_id = $2", name, userID+i)
			return err
		})
	}

	err = b.RunSequential(ctx)
	if err != nil {
		return fmt.Errorf("batchSeq RunSequential: %w", err)
	}
	*duration += time.Since(batchSeqStartTime)

	return nil
}

func measureSeq(ctx context.Context, db *dbbatch.BatchDB, duration *time.Duration, execCount int, userID int64) (err error) {
	const name = "second seq"

	seqStartTime := time.Now()
	for i := int64(0); i < int64(execCount); i++ {
		_, err = db.ExecContext(ctx, "update items set name = $1 where user_id = $2", name, userID+i)
		if err != nil {
			return fmt.Errorf("seq ExecContext: %w", err)
		}
	}
	*duration += time.Since(seqStartTime)

	return nil
}

func measureSeqSingleConn(ctx context.Context, db *dbbatch.BatchDB, duration *time.Duration, execCount int, userID int64) (err error) {
	const name = "second seq single conn"

	seqSingleConnStartTime := time.Now()
	var conn *sql.Conn
	conn, err = db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("error in db.Conn: %w", err)
	}
	for i := int64(0); i < int64(execCount); i++ {
		_, err = conn.ExecContext(ctx, "update items set name = $1 where user_id = $2", name, userID+i)
		if err != nil {
			return fmt.Errorf("error in conn.ExecContext: %w", err)
		}
	}
	err = conn.Close()
	if err != nil {
		return fmt.Errorf("seqSingleConn close: %w", err)
	}
	*duration += time.Since(seqSingleConnStartTime)

	return nil
}

func measureUpsert(ctx context.Context, db *dbbatch.BatchDB, duration *time.Duration, _ int, _ int64) (err error) {
	const name = "second"

	var users []Item
	err = db.Select(&users, "select * from items")
	if err != nil {
		return fmt.Errorf("upsert Select: %w", err)
	}
	for i := range users {
		users[i].Name = name + " upsert"
	}

	upsertStartTime := time.Now()
	_, err = db.NamedExecContext(
		ctx,
		`insert into items (id, name, user_id) values (:id, :name, :user_id)
				on conflict(id) do update set name = EXCLUDED.name, user_id = EXCLUDED.user_id`,
		users,
	)
	if err != nil {
		return fmt.Errorf("upsert NamedExecContext: %w", err)
	}

	*duration += time.Since(upsertStartTime)

	return nil
}

func Perf(ctx context.Context, t *testing.T, db *dbbatch.BatchDB, iterationCount, execCount int) {
	err := PrepareDB(ctx, db)
	if err != nil {
		panic(fmt.Errorf("PrepareDB: %w", err))
	}

	res, err := PerfToResult(ctx, db, iterationCount, execCount)
	require.NoError(t, err)

	fmt.Printf(
		"| %-10s | %-10s | %-10s | %-16s | %-10s | %-10s | %-10s | %-10s |\n"+
			"| %-10d | %-10d | %-10s | %-16s | %-10s | %-10s | %-10s | %-10.3f |\n",
		"iterations", "execs", "seq", "seq single conn", "batch", "batch seq", "upsert", "rate batch/seq",
		iterationCount,
		execCount,
		res.Seq,
		res.SeqSingleConn,
		res.Batch,
		res.BatchSeq,
		res.Upsert,
		res.SeqBatchRate,
	)
}
