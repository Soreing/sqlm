package sqlm

import (
	"context"
	"database/sql"
)

type Conn struct {
	cn   *sql.Conn
	mdws hndl
}

// Connection returns the underlying *sql.Conn object.
func (cn *Conn) Connection() *sql.Conn {
	return cn.cn
}

// BeginTx creates a new transaction *Tx object with options. It calls
// sql.BeginTx and stores a *sql.Tx object internally. The transaction
// inherits the middlewares of the database object.
func (cn *Conn) BeginTx(ctx context.Context, opts *sql.TxOptions) (*Tx, error) {
	mdws := cn.mdws.fnHndl(FN_Begin)
	if len(mdws) == 0 {
		if sqltx, err := cn.cn.BeginTx(ctx, opts); err != nil {
			return nil, err
		} else {
			return &Tx{sqltx, cn.mdws, false}, nil
		}
	}

	var tx *Tx
	var err error
	qctx := newContext(ctx, FN_Begin, SRC_Connection, "", nil, mdws)
	qctx.fn = func() {
		if t, e := cn.cn.BeginTx(ctx, opts); e != nil {
			qctx.Error(e)
		} else {
			tx = &Tx{t, cn.mdws, false}
		}
	}

	qctx.Next()
	err = qctx.fsterr()
	return tx, err
}

// Close returns the connection to the connection pool. It calls sql.Close.
func (cn *Conn) Close() error {
	return cn.cn.Close()
}

// ExecContext executes a query without returning any rows The args are for any
// placeholder parameters in the query. It calls sql.ExecContext.
func (cn *Conn) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	mdws := cn.mdws.fnHndl(FN_Exec)
	if len(mdws) == 0 {
		return cn.cn.ExecContext(ctx, query, args...)
	}

	var res sql.Result
	var err error
	qctx := newContext(ctx, FN_Exec, SRC_Connection, query, args, mdws)
	qctx.fn = func() {
		if r, e := cn.cn.ExecContext(ctx, qctx.Query, qctx.Args...); e != nil {
			qctx.Error(e)
		} else {
			res = r
		}
	}

	qctx.Next()
	err = qctx.fsterr()
	return res, err
}

// PingContext verifies a connection to the database is still alive,
// establishing a connection if necessary. It calls sql.PingContext.
func (cn *Conn) PingContext(ctx context.Context) error {
	mdws := cn.mdws.fnHndl(FN_Ping)
	if len(mdws) == 0 {
		return cn.cn.PingContext(ctx)
	}

	var err error
	qctx := newContext(ctx, FN_Ping, SRC_Connection, "", nil, mdws)
	qctx.fn = func() {
		if e := cn.cn.PingContext(ctx); e != nil {
			qctx.Error(e)
		}
	}

	qctx.Next()
	err = qctx.fsterr()
	return err
}

// PrepareContext creates a prepared statement for later queries or executions.
// It calls sql.PrepareContext and stores a *sql.Stmt object internally. The
// statement inherits the middlewares of the database object.
func (cn *Conn) PrepareContext(ctx context.Context, query string) (*Stmt, error) {
	mdws := cn.mdws.fnHndl(FN_Prepare)
	if len(mdws) == 0 {
		if sqlstmt, err := cn.cn.PrepareContext(ctx, query); err != nil {
			return nil, err
		} else {
			return &Stmt{sqlstmt, cn.mdws, query}, nil
		}
	}

	var stmt *Stmt
	var err error
	qctx := newContext(ctx, FN_Prepare, SRC_Connection, query, nil, mdws)
	qctx.fn = func() {
		if s, e := cn.cn.PrepareContext(ctx, qctx.Query); e != nil {
			qctx.Error(e)
		} else {
			stmt = &Stmt{s, cn.mdws, query}
		}
	}

	qctx.Next()
	err = qctx.fsterr()
	return stmt, err
}

// QueryContext executes a query that returns rows, typically a SELECT.
// The args are for any placeholder parameters in the query. It calls
// sql.QueryContext
func (cn *Conn) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	mdws := cn.mdws.fnHndl(FN_Query)
	if len(mdws) == 0 {
		return cn.cn.QueryContext(ctx, query, args...)
	}

	var rows *sql.Rows
	var err error
	qctx := newContext(ctx, FN_Query, SRC_Connection, query, args, mdws)
	qctx.fn = func() {
		if r, e := cn.cn.QueryContext(ctx, qctx.Query, qctx.Args...); e != nil {
			qctx.Error(e)
		} else {
			rows = r
		}
	}

	qctx.Next()
	err = qctx.fsterr()
	return rows, err
}
