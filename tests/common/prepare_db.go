package common

import (
	"context"
	"time"

	"github.com/inna-maikut/dbbatch"
)

type Item struct {
	ID         int64     `db:"id"`
	Name       string    `db:"name"`
	UserID     int64     `db:"user_id"`
	CreateTime time.Time `db:"create_time"`
}

func PrepareDB(ctx context.Context, db *dbbatch.BatchDB) error {
	_, err := db.ExecContext(ctx, "drop table if exists items")
	if err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, `create table items (
		id bigserial primary key,
		name text,
		user_id bigint not null,
		create_time timestamp with time zone default now()
    )`)
	return err
}
