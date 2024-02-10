//go:build integration

package pgx_v5

import (
	"testing"

	"github.com/inna-maikut/dbbatch/tests/common"
)

func TestPgxV4_Perf_10x1000(t *testing.T) {
	ctx, db := setup(t, false)

	common.Perf(ctx, t, db, 10, 1000)
}

func TestPgxV4_Perf_1000x100(t *testing.T) {
	ctx, db := setup(t, false)

	common.Perf(ctx, t, db, 1000, 100)
}

func TestPgxV4_Perf_100x100(t *testing.T) {
	ctx, db := setup(t, false)

	common.Perf(ctx, t, db, 100, 100)
}

func TestPgxV4_Perf_1000x10(t *testing.T) {
	ctx, db := setup(t, false)

	common.Perf(ctx, t, db, 1000, 10)
}

func TestPgxV4_Perf_5000x2(t *testing.T) {
	ctx, db := setup(t, false)

	common.Perf(ctx, t, db, 5000, 2)
}
