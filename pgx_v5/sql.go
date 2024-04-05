package pgx_v5

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"

	"github.com/inna-maikut/dbbatch"
)

var batchPgxDriver *Driver

var (
	_ driver.DriverContext        = &Driver{}
	_ driver.Driver               = &Driver{}
	_ dbbatch.BatchRequestsSender = &Conn{}
	_ driver.Conn                 = &Conn{}
)

func init() {
	batchPgxDriver = &Driver{}

	sql.Register("batch_pgx", batchPgxDriver)
	sql.Register("batch_pgx_v5", batchPgxDriver)
}

// GetDefaultDriver returns the driver initialized in the init function
// and used when the batch_pgx driver is registered.
func GetDefaultDriver() driver.Driver {
	return batchPgxDriver
}

type Driver struct{}

func (d *Driver) Open(name string) (driver.Conn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second) // Ensure eventual timeout
	defer cancel()

	connector, err := d.OpenConnector(name)
	if err != nil {
		return nil, err
	}
	return connector.Connect(ctx)
}

func (d *Driver) OpenConnector(name string) (driver.Connector, error) {
	pgxDriver := stdlib.GetDefaultDriver()
	pgxDriverConnector, ok := pgxDriver.(driver.DriverContext)
	if !ok {
		return nil, errors.New("pgx driver is not driver.DriverContext interface")
	}
	pgxConnector, err := pgxDriverConnector.OpenConnector(name)
	if err != nil {
		return nil, err
	}
	return &driverConnector{base: pgxConnector, driver: d, name: name}, nil
}

type driverConnector struct {
	base driver.Connector

	driver *Driver
	name   string
}

func (dc *driverConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := dc.base.Connect(ctx)
	if err != nil {
		return nil, err
	}

	var pgxConn *pgx.Conn
	if getter, ok := conn.(PgxConnGetter); ok {
		pgxConn = getter.Conn()
	}

	return &Conn{
		base: conn,
		conn: pgxConn,
	}, nil
}

func (dc *driverConnector) Driver() driver.Driver {
	return dc.driver
}

type Conn struct {
	base driver.Conn
	conn *pgx.Conn
}

func (c *Conn) SendBatchRequests(ctx context.Context, requests []dbbatch.Request) (res any, close func() error, err error) {
	b := pgx.Batch{}
	for _, request := range requests {
		b.Queue(request.Query, request.Args...)
	}

	batchResults := c.conn.SendBatch(ctx, &b)

	return batchResults, batchResults.Close, nil
}

func (c *Conn) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}

func (c *Conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	if dbbatch.BatchConnFromContext(ctx) != nil {
		return nil, errors.New("prepared statements not supported in batch")
	}

	if ext, ok := c.base.(interface {
		PrepareContext(ctx context.Context, query string) (driver.Stmt, error)
	}); ok {
		return ext.PrepareContext(ctx, query)
	}
	return c.base.Prepare(query)
}

func (c *Conn) Close() error {
	return c.base.Close()
}

func (c *Conn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *Conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if dbbatch.BatchConnFromContext(ctx) != nil {
		return nil, errors.New("transactions are not supported in batch")
	}

	if ext, ok := c.base.(interface {
		BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error)
	}); ok {
		return ext.BeginTx(ctx, opts)
	}
	return c.base.Begin() //nolint:staticcheck
}

func (c *Conn) ExecContext(ctx context.Context, query string, argsV []driver.NamedValue) (driver.Result, error) {
	b := dbbatch.BatchConnFromContext(ctx)
	if c.conn == nil || b == nil {
		if ext, ok := c.base.(interface {
			ExecContext(ctx context.Context, query string, argsV []driver.NamedValue) (driver.Result, error)
		}); ok {
			return ext.ExecContext(ctx, query, argsV)
		}
		stmt, err := c.base.Prepare(query)
		if err != nil {
			return nil, err
		}

		args := namedValueToDriverValue(argsV)
		return stmt.Exec(args) //nolint:staticcheck
	}

	if c.conn.IsClosed() {
		return nil, driver.ErrBadConn
	}

	args := namedValueToInterface(argsV)

	res := b.BatchRunner().Queue(dbbatch.Request{
		Query: query,
		Args:  args,
	})
	batchResults, ok := res.(pgx.BatchResults)
	if !ok {
		return nil, fmt.Errorf("unknown type of pgx.BatchResults: %+v", res)
	}
	commandTag, err := batchResults.Exec()
	if err != nil {
		return nil, fmt.Errorf("batchResults.Exec: %w", err)
	}

	return driver.RowsAffected(commandTag.RowsAffected()), nil
}

func (c *Conn) QueryContext(ctx context.Context, query string, argsV []driver.NamedValue) (driver.Rows, error) {
	bc := dbbatch.BatchConnFromContext(ctx)
	if c.conn == nil || bc == nil {
		if ext, ok := c.base.(interface {
			QueryContext(ctx context.Context, query string, argsV []driver.NamedValue) (driver.Rows, error)
		}); ok {
			return ext.QueryContext(ctx, query, argsV)
		}
		stmt, err := c.base.Prepare(query)
		if err != nil {
			return nil, err
		}

		args := namedValueToDriverValue(argsV)
		return stmt.Query(args) //nolint:staticcheck
	}

	if c.conn.IsClosed() {
		return nil, driver.ErrBadConn
	}

	args := make([]any, 0, len(argsV))
	args = append(args, namedValueToInterface(argsV)...)

	res := bc.BatchRunner().Queue(dbbatch.Request{
		Query: query,
		Args:  args,
	})
	batchResults, ok := res.(pgx.BatchResults)
	if !ok {
		return nil, fmt.Errorf("unknown type of pgx.BatchResults: %+v", res)
	}
	rows, err := batchResults.Query()
	if err != nil {
		return nil, fmt.Errorf("batchResults.Query: %w", err)
	}

	// Preload first row because otherwise we won't know what columns are available when database/sql asks.
	more := rows.Next()
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	return &Rows{conn: c, rows: rows, skipNext: true, skipNextMore: more}, nil
}

func (c *Conn) Ping(ctx context.Context) error {
	if ext, ok := c.base.(interface {
		Ping(ctx context.Context) error
	}); ok {
		return ext.Ping(ctx)
	}
	return errors.New("Conn.Ping is not implemented")
}

func (c *Conn) CheckNamedValue(*driver.NamedValue) error {
	// Underlying pgx supports sql.Scanner and driver.Valuer interfaces natively. So everything can be passed through directly.
	return nil
}

func (c *Conn) ResetSession(_ context.Context) error {
	if c.conn.IsClosed() {
		return driver.ErrBadConn
	}

	return nil
}

type rowValueFunc func(src []byte) (driver.Value, error)

func namedValueToInterface(argsV []driver.NamedValue) []any {
	args := make([]any, 0, len(argsV))
	for _, v := range argsV {
		if v.Value != nil {
			args = append(args, v.Value.(any))
		} else {
			args = append(args, nil)
		}
	}
	return args
}

func namedValueToDriverValue(argsV []driver.NamedValue) []driver.Value {
	args := make([]driver.Value, 0, len(argsV))
	for _, v := range argsV {
		if v.Value != nil {
			args = append(args, v.Value.(any))
		} else {
			args = append(args, nil)
		}
	}
	return args
}
