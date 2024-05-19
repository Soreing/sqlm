package sqlm

import (
	"context"
	"database/sql"
)

type Tx struct {
	tx   *sql.Tx
	mdws hndl
	done bool
}

// Transaction returns the underlying *sql.Tx object.
func (tx *Tx) Transaction() *sql.Tx {
	return tx.tx
}

// Commit calls CommitContext with context.Background.
func (tx *Tx) Commit() error {
	return tx.CommitContext(context.Background())
}

// CommitContext commits the transaction. It calls sql.Commit. If the
// transaction has already been committed or rolled back, it's a noop and
// nothing will execute.
func (tx *Tx) CommitContext(ctx context.Context) error {
	mdws := tx.mdws.fnHndl(FN_Commit)
	if len(mdws) == 0 {
		return tx.tx.Commit()
	}

	var err error
	qctx := newContext(ctx, FN_Commit, SRC_Transaction, "", nil, mdws)
	qctx.fn = func() {
		if e := tx.tx.Commit(); e != nil {
			qctx.Error(e)
		}
	}

	qctx.Next()
	err = qctx.fsterr()
	return err
}

// Rollback calls RollbackContext with context.Background.
func (tx *Tx) Rollback() error {
	return tx.RollbackContext(context.Background())
}

// Rollback aborts the transaction. It calls sql.Rollback. If the transaction
// has already been committed or rolled back, it's a noop and nothing will
// execute.
func (tx *Tx) RollbackContext(ctx context.Context) error {
	mdws := tx.mdws.fnHndl(FN_Rollback)
	if len(mdws) == 0 {
		return tx.tx.Rollback()
	}

	var err error
	qctx := newContext(ctx, FN_Rollback, SRC_Transaction, "", nil, mdws)
	qctx.fn = func() {
		if e := tx.tx.Rollback(); e != nil {
			qctx.Error(e)
		}
	}

	qctx.Next()
	err = qctx.fsterr()
	return err
}

// Exec calls ExecContext with context.Background, query and args.
func (tx *Tx) Exec(query string, args ...any) (sql.Result, error) {
	return tx.ExecContext(context.Background(), query, args...)
}

// ExecContext executes a query without returning any rows The args are for any
// placeholder parameters in the query. It calls sql.ExecContext.
func (tx *Tx) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	mdws := tx.mdws.fnHndl(FN_Exec)
	if len(mdws) == 0 {
		return tx.tx.ExecContext(ctx, query, args...)
	}

	var res sql.Result
	var err error
	qctx := newContext(ctx, FN_Exec, SRC_Transaction, query, args, mdws)
	qctx.fn = func() {
		if r, e := tx.tx.ExecContext(ctx, qctx.Query, qctx.Args...); e != nil {
			qctx.Error(e)
		} else {
			res = r
		}
	}

	qctx.Next()
	err = qctx.fsterr()
	return res, err
}

// Prepare calls PrepareContext with context.Background and query.
func (tx *Tx) Prepare(query string) (*Stmt, error) {
	return tx.PrepareContext(context.Background(), query)
}

// PrepareContext creates a prepared statement for later queries or executions.
// It calls sql.PrepareContext and stores a *sql.Stmt object internally. The
// statement inherits the middlewares of the database object.
func (tx *Tx) PrepareContext(ctx context.Context, query string) (*Stmt, error) {
	mdws := tx.mdws.fnHndl(FN_Prepare)
	if len(mdws) == 0 {
		if sqlstmt, err := tx.tx.PrepareContext(ctx, query); err != nil {
			return nil, err
		} else {
			return &Stmt{sqlstmt, tx.mdws, query}, nil
		}
	}

	var stmt *Stmt
	var err error
	qctx := newContext(ctx, FN_Prepare, SRC_Transaction, query, nil, mdws)
	qctx.fn = func() {
		if s, e := tx.tx.PrepareContext(ctx, qctx.Query); e != nil {
			qctx.Error(e)
		} else {
			stmt = &Stmt{s, tx.mdws, query}
		}
	}

	qctx.Next()
	err = qctx.fsterr()
	return stmt, err
}

// Query calls QueryContext with context.Background, query and args.
func (tx *Tx) Query(query string, args ...any) (*sql.Rows, error) {
	return tx.QueryContext(context.Background(), query, args...)
}

// QueryContext executes a query that returns rows, typically a SELECT.
// The args are for any placeholder parameters in the query. It calls
// sql.QueryContext
func (tx *Tx) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	mdws := tx.mdws.fnHndl(FN_Query)
	if len(mdws) == 0 {
		return tx.tx.QueryContext(ctx, query, args...)
	}

	var rows *sql.Rows
	var err error
	qctx := newContext(ctx, FN_Query, SRC_Transaction, query, args, mdws)
	qctx.fn = func() {
		if r, e := tx.tx.QueryContext(ctx, qctx.Query, qctx.Args...); e != nil {
			qctx.Error(e)
		} else {
			rows = r
		}
	}

	qctx.Next()
	err = qctx.fsterr()
	return rows, err
}

// TODO
func (tx *Tx) Stmt(stmt *Stmt) *Stmt {
	panic("unimplemented")
}

// TODO
func (tx *Tx) StmtContext(ctx context.Context, stmt *Stmt) *Stmt {
	panic("unimplemented")
}
