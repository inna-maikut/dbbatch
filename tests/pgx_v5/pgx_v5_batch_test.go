//go:build integration

package pgx_v5

import (
	"testing"

	"github.com/inna-maikut/dbbatch/tests/common"
)

func TestPgxV4_BatchOneStep(t *testing.T) {
	ctx, db := setup(t, false)

	common.BatchOneStep(ctx, t, db)
}

func TestPgxV4_BatchMultiStep(t *testing.T) {
	ctx, db := setup(t, false)

	common.BatchMultiStep(ctx, t, db)
}

func TestPgxV4_BatchTx(t *testing.T) {
	ctx, db := setup(t, false)

	common.BatchTx(ctx, t, db)
}

func TestPgxV4_BatchManyTimes(t *testing.T) {
	ctx, db := setup(t, false)

	common.BatchManyTimes(ctx, t, db)
}
