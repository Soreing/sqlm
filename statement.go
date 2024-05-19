package sqlm

import (
	"context"
	"database/sql"
)

type Stmt struct {
	st    *sql.Stmt
	mdws  hndl
	query string
}

// Statement returns the underlying *sql.Stmt object.
func (st *Stmt) Statement() *sql.Stmt {
	return st.st
}

// Close closes the statement. It calls sql.Close.
func (st *Stmt) Close() error {
	return st.st.Close()
}

// Exec calls ExecContext with context.Background and args.
func (st *Stmt) Exec(query string, args ...any) (sql.Result, error) {
	return st.ExecContext(context.Background(), args...)
}

// ExecContext ExecContext executes a prepared statement with the given
// arguments. It calls sql.ExecContext. The query string is passed to the
// query context, but changes to it will not change the prepared statement.
func (st *Stmt) ExecContext(ctx context.Context, args ...any) (sql.Result, error) {
	mdws := st.mdws.fnHndl(FN_Exec)
	if len(mdws) == 0 {
		return st.st.ExecContext(ctx, args...)
	}

	var res sql.Result
	var err error
	qctx := newContext(ctx, FN_Exec, SRC_Statement, st.query, args, mdws)
	qctx.fn = func() {
		if r, e := st.st.ExecContext(ctx, qctx.Args...); e != nil {
			qctx.Error(e)
		} else {
			res = r
		}
	}

	qctx.Next()
	err = qctx.fsterr()
	return res, err
}

// Query calls QueryContext with context.Background and args.
func (st *Stmt) Query(args ...any) (*sql.Rows, error) {
	return st.QueryContext(context.Background(), args...)
}

// QueryContext executes a prepared query statement with the given arguments.
// It calls sql.QueryContext. The query string is passed to the query context,
// but changes to it will not change the prepared statement.
func (st *Stmt) QueryContext(ctx context.Context, args ...any) (*sql.Rows, error) {
	mdws := st.mdws.fnHndl(FN_Query)
	if len(mdws) == 0 {
		return st.st.QueryContext(ctx, args...)
	}

	var rows *sql.Rows
	var err error
	qctx := newContext(ctx, FN_Query, SRC_Statement, st.query, args, mdws)
	qctx.fn = func() {
		if r, e := st.st.QueryContext(ctx, qctx.Args...); e != nil {
			qctx.Error(e)
		} else {
			rows = r
		}
	}

	qctx.Next()
	err = qctx.fsterr()
	return rows, err
}
