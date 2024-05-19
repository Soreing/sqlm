package sqlm

import (
	"context"
	"sync"
)

// handler describes a middleware handler's function signature.
type handler func(context.Context, *Context)

// Context is a collection of data used in the process of calling various sql
// functions. It enables middleware handlers to modify the parameters of the
// called sql functions or extend their functionality. Context DOES NOT
// implement context.Context, and is not used for calling any underlying sql
// functions.
type Context struct {
	funct  Function
	source Source
	Query  string
	Args   []any
	fn     func()
	errs   []error

	mdws   []handler
	mdwIdx int

	ctx    context.Context
	mtx    *sync.Mutex
	Values map[string]any
}

// newContext creates a new context from values.
func newContext(
	ctx context.Context,
	funct Function,
	source Source,
	query string,
	args []any,
	mdws []handler,
) *Context {
	return &Context{
		funct:  funct,
		Query:  query,
		Args:   args,
		errs:   make([]error, 0, 1),
		mdws:   mdws,
		mdwIdx: 0,
		ctx:    ctx,
		mtx:    &sync.Mutex{},
		Values: map[string]any{},
	}
}

// Function returns which sql function is being executed in the operation.
func (ctx *Context) Function() Function {
	return ctx.funct
}

// Source returns which sql object initiated the operation.
func (ctx *Context) Source() Source {
	return ctx.source
}

// Lock locks the mutex within the context
func (ctx *Context) Lock(key string, val any) {
	ctx.mtx.Lock()
}

// Unlock unlocks the mutex within the context
func (ctx *Context) Unlock(key string, val any) {
	ctx.mtx.Unlock()
}

// Set assigns some value to a key in the context's Values store. The operation
// locks the context's mutex for thread safety.
func (ctx *Context) Set(key string, val any) {
	ctx.mtx.Lock()
	ctx.Values[key] = val
	ctx.mtx.Unlock()
}

// Get retrieves some value from the context's Values store by a key. The
// operation locks the context's mutex for thread safety.
func (ctx *Context) Get(key string) (val any, ok bool) {
	ctx.mtx.Lock()
	val, ok = ctx.Values[key]
	ctx.mtx.Unlock()
	return val, ok
}

// Delete deletes the value from the context's Values under some key. The
// operation locks the context's mutex for thread safety.
func (ctx *Context) Delete(key string) {
	ctx.mtx.Lock()
	delete(ctx.Values, key)
	ctx.mtx.Unlock()
}

// Next calls the next handler on the middleware chain.
func (ctx *Context) Next() {
	idx := ctx.mdwIdx
	ctx.mdwIdx++
	if idx < len(ctx.mdws) {
		ctx.mdws[idx](ctx.ctx, ctx)
	} else {
		ctx.fn()
	}
}

// Error appends an error to the list of errors in the context. Only the first
// error will be returned in the sql function. If the sql function returned an
// error, it will be added to the error list.
func (ctx *Context) Error(err error) {
	ctx.errs = append(ctx.errs, err)
}

// Errors returns the list of errors in the context.
func (ctx *Context) Errors() []error {
	return ctx.errs
}

// fsterr returns the first error in the context or nil if empty.
func (ctx *Context) fsterr() error {
	if len(ctx.errs) != 0 {
		return ctx.errs[0]
	} else {
		return nil
	}
}
