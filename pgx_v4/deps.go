package pgx_v4

import "github.com/jackc/pgx/v4"

type PgxConnGetter interface {
	Conn() *pgx.Conn
}
