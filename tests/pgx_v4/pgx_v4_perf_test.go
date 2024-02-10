//go:build integration

package pgx_v4

import (
	"testing"

	"github.com/inna-maikut/dbbatch/tests/common"
)

func TestPgxV4_Perf_10x10000(t *testing.T) {
	ctx, db := setup(t, false)

	common.Perf(ctx, t, db, 1, 10000)
}

func TestPgxV4_Perf_10x5000(t *testing.T) {
	ctx, db := setup(t, false)

	common.Perf(ctx, t, db, 1, 5000)
}

func TestPgxV4_Perf_10x2000(t *testing.T) {
	ctx, db := setup(t, false)

	common.Perf(ctx, t, db, 2, 2000)
}

func TestPgxV4_Perf_10x1000(t *testing.T) {
	ctx, db := setup(t, false)

	common.Perf(ctx, t, db, 2, 1000)
}

func TestPgxV4_Perf_20x500(t *testing.T) {
	ctx, db := setup(t, false)

	common.Perf(ctx, t, db, 2, 500)
}

func TestPgxV4_Perf_50x200(t *testing.T) {
	ctx, db := setup(t, false)

	common.Perf(ctx, t, db, 5, 200)
}

func TestPgxV4_Perf_100x100(t *testing.T) {
	ctx, db := setup(t, false)

	common.Perf(ctx, t, db, 10, 100)
}

func TestPgxV4_Perf_200x50(t *testing.T) {
	ctx, db := setup(t, false)

	common.Perf(ctx, t, db, 20, 50)
}

func TestPgxV4_Perf_500x20(t *testing.T) {
	ctx, db := setup(t, false)

	common.Perf(ctx, t, db, 50, 20)
}

func TestPgxV4_Perf_1000x10(t *testing.T) {
	ctx, db := setup(t, false)

	common.Perf(ctx, t, db, 100, 10)
}

func TestPgxV4_Perf_2000x5(t *testing.T) {
	ctx, db := setup(t, false)

	common.Perf(ctx, t, db, 200, 5)
}

func TestPgxV4_Perf_5000x2(t *testing.T) {
	ctx, db := setup(t, false)

	common.Perf(ctx, t, db, 500, 2)
}

func TestPgxV4_Perf_10000x1(t *testing.T) {
	ctx, db := setup(t, false)

	common.Perf(ctx, t, db, 1000, 1)
}

func TestPgxV4_Perf_Big_1000x100(t *testing.T) {
	ctx, db := setup(t, false)

	common.Perf(ctx, t, db, 100, 100)
}
