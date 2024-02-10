package pgx_v5

import (
	"github.com/jackc/pgx/v5"
)

type PgxConnGetter interface {
	Conn() *pgx.Conn
}
