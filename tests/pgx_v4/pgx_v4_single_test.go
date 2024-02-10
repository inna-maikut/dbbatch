//go:build integration

package pgx_v4

import (
	"testing"

	"github.com/inna-maikut/dbbatch/tests/common"
)

func TestPgxV4_Single(t *testing.T) {
	ctx, db := setup(t, false)

	common.Single(ctx, t, db)
}

func TestPgxV4_SingleCancel(t *testing.T) {
	ctx, db := setup(t, false)

	common.SingleCancel(ctx, t, db)
}

func TestPgxV4_SingleNoCancel(t *testing.T) {
	ctx, db := setup(t, true)

	common.SingleNotCancel(ctx, t, db)
}
