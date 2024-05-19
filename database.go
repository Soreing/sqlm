package sqlm

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"time"
)

// hndl is an interface for getting middleware handlers for a sql function.
type hndl interface {
	fnHndl(fn Function) []handler
}

// DB is a wrapper class around sql.DB with middleware support. Middleware
// handlers can be attached on different sql functions to extend their
// features.
type DB struct {
	db   *sql.DB
	mdws map[Function][]handler
}

// Database returns the underlying *sql.DB object.
func (db *DB) Database() *sql.DB {
	return db.db
}

// Open creates a new database *DB object from a driver and data source.
// It calls sql.Open and stores a *sql.DB object internally
func Open(driverName string, dataSourceName string) (*DB, error) {
	db, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		return nil, err
	}

	return &DB{
		db:   db,
		mdws: map[Function][]handler{},
	}, nil
}

// OpenDB creates a new database *DB object from a driver.Connector.
// It calls sql.OpenDB  and stores a *sql.DB object internally
func OpenDB(c driver.Connector) *DB {
	return &DB{
		db:   sql.OpenDB(c),
		mdws: map[Function][]handler{},
	}
}

// Use attaches a middleware handler to specific sql functions. The function
// panics if the handler is nil, or the list of functions is empty. Handlers
// should be added in advance in a setup phase. The function is not thread safe.
func (db *DB) Use(mdw func(context.Context, *Context), fns []Function) {
	if len(fns) == 0 {
		panic("function list is empty")
	} else if mdw == nil {
		panic("middleware is nil")
	}

	for _, fn := range fns {
		db.mdws[fn] = append(db.mdws[fn], mdw)
	}
}

// Begin calls BeginTx with context.Background and no options.
func (db *DB) Begin() (*Tx, error) {
	return db.BeginTx(context.Background(), nil)
}

// BeginTx creates a new transaction *Tx object with options. It calls
// sql.BeginTx and stores a *sql.Tx object internally. The transaction
// inherits the middlewares of the database object.
func (db *DB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*Tx, error) {
	mdws := db.mdws[FN_Begin]
	if len(mdws) == 0 {
		if sqltx, err := db.db.BeginTx(ctx, opts); err != nil {
			return nil, err
		} else {
			return &Tx{sqltx, db, false}, nil
		}
	}

	var tx *Tx
	var err error
	qctx := newContext(ctx, FN_Begin, SRC_Database, "", nil, mdws)
	qctx.fn = func() {
		if t, e := db.db.BeginTx(ctx, opts); e != nil {
			qctx.Error(e)
		} else {
			tx = &Tx{t, db, false}
		}
	}

	qctx.Next()
	err = qctx.fsterr()
	return tx, err
}

// Close closes the database connection. It calls sql.Close.
func (db *DB) Close() error {
	return db.db.Close()
}

// Conn returns a single connection *Conn object. It calls sql.Conn. The
// connection inherits the middlewares of the database object.
func (db *DB) Conn(ctx context.Context) (*Conn, error) {
	conn, err := db.db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	return &Conn{conn, db}, nil
}

// Driver returns the database's underlying driver. It calls sql.Driver.
func (db *DB) Driver() driver.Driver {
	return db.db.Driver()
}

// Exec calls ExecContext with context.Background, query and args.
func (db *DB) Exec(query string, args ...any) (sql.Result, error) {
	return db.ExecContext(context.Background(), query, args...)
}

// ExecContext executes a query without returning any rows The args are for any
// placeholder parameters in the query. It calls sql.ExecContext.
func (db *DB) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	mdws := db.mdws[FN_Exec]
	if len(mdws) == 0 {
		return db.db.ExecContext(ctx, query, args...)
	}

	var res sql.Result
	var err error
	qctx := newContext(ctx, FN_Exec, SRC_Database, query, args, mdws)
	qctx.fn = func() {
		if r, e := db.db.ExecContext(ctx, qctx.Query, qctx.Args...); e != nil {
			qctx.Error(e)
		} else {
			res = r
		}
	}

	qctx.Next()
	err = qctx.fsterr()
	return res, err
}

// Ping calls PingContext with context.Background.
func (db *DB) Ping() error {
	return db.PingContext(context.Background())
}

// PingContext verifies a connection to the database is still alive,
// establishing a connection if necessary. It calls sql.PingContext.
func (db *DB) PingContext(ctx context.Context) error {
	mdws := db.mdws[FN_Ping]
	if len(mdws) == 0 {
		return db.db.PingContext(ctx)
	}

	var err error
	qctx := newContext(ctx, FN_Ping, SRC_Database, "", nil, mdws)
	qctx.fn = func() {
		if e := db.db.PingContext(ctx); e != nil {
			qctx.Error(e)
		}
	}

	qctx.Next()
	err = qctx.fsterr()
	return err
}

// Prepare calls PrepareContext with context.Background and query.
func (db *DB) Prepare(query string) (*Stmt, error) {
	return db.PrepareContext(context.Background(), query)
}

// PrepareContext creates a prepared statement for later queries or executions.
// It calls sql.PrepareContext and stores a *sql.Stmt object internally. The
// statement inherits the middlewares of the database object.
func (db *DB) PrepareContext(ctx context.Context, query string) (*Stmt, error) {
	mdws := db.mdws[FN_Prepare]
	if len(mdws) == 0 {
		if sqlstmt, err := db.db.PrepareContext(ctx, query); err != nil {
			return nil, err
		} else {
			return &Stmt{sqlstmt, db, query}, nil
		}
	}

	var stmt *Stmt
	var err error
	qctx := newContext(ctx, FN_Prepare, SRC_Database, query, nil, mdws)
	qctx.fn = func() {
		if s, e := db.db.PrepareContext(ctx, qctx.Query); e != nil {
			qctx.Error(e)
		} else {
			stmt = &Stmt{s, db, query}
		}
	}

	qctx.Next()
	err = qctx.fsterr()
	return stmt, err
}

// Query calls QueryContext with context.Background, query and args.
func (db *DB) Query(query string, args ...any) (*sql.Rows, error) {
	return db.QueryContext(context.Background(), query, args...)
}

// QueryContext executes a query that returns rows, typically a SELECT.
// The args are for any placeholder parameters in the query. It calls
// sql.QueryContext
func (db *DB) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	mdws := db.mdws[FN_Query]
	if len(mdws) == 0 {
		return db.db.QueryContext(ctx, query, args...)
	}

	var rows *sql.Rows
	var err error
	qctx := newContext(ctx, FN_Query, SRC_Database, query, args, mdws)
	qctx.fn = func() {
		if r, e := db.db.QueryContext(ctx, qctx.Query, qctx.Args...); e != nil {
			qctx.Error(e)
		} else {
			rows = r
		}
	}

	qctx.Next()
	err = qctx.fsterr()
	return rows, err
}

// SetConnMaxIdleTime sets the maximum amount of time a connection may be idle.
// It calls sql.SetConnMaxIdleTime.
func (db *DB) SetConnMaxIdleTime(d time.Duration) {
	db.db.SetConnMaxIdleTime(d)
}

// SetConnMaxLifetime sets the maximum amount of time a connection may be
// reused. It calls sql.SetConnMaxLifetime.
func (db *DB) SetConnMaxLifetime(d time.Duration) {
	db.db.SetConnMaxLifetime(d)
}

// SetMaxIdleConns sets the maximum number of connections in the idle connection
// pool. It calls sql.SetMaxIdleConns.
func (db *DB) SetMaxIdleConns(n int) {
	db.db.SetMaxIdleConns(n)
}

// SetMaxOpenConns sets the maximum number of open connections to the database.
// It calls sql.SetMaxOpenConns.
func (db *DB) SetMaxOpenConns(n int) {
	db.db.SetMaxOpenConns(n)
}

// Stats returns database statistics. It calls sql.Stats
func (db *DB) Stats(n int) sql.DBStats {
	return db.db.Stats()
}

// fnHndl returns middleware handlers for a sql function
func (db *DB) fnHndl(fn Function) []handler {
	return db.mdws[fn]
}
