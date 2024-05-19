# SQLM
SQLM is a light wrapper around the native sql package, providing middleware
support to extend the features of specific functions.

## Usage
Get a Database object through connection. The examples use a PostgreSQL database.
```golang
dsn := "host=127.0.0.1 port=5432 user=postgres password=postgres dbname=library sslmode=disable"
db, err := sqlm.Open("postgres", dsn)
if err != nil {
	panic(err)
}
```

SQLM supports most of the native functions from database/sql, except QueryRow,
because internally calls Query anyway, and *sql.Row is terrible.
```golang
ctx := context.Background()

tx, err := db.BeginTx(ctx, nil)
if err != nil {
    panic(err)
}

id := "1d48e3ab-951d-404d-8e54-fa2d4f5f6bb6"
query := "INSERT INTO books(id, title) VALUES($1, $2) RETURNING id, title"
rows, err := tx.QueryContext(ctx, query, id, "Beards and Beer")
if err != nil {
    tx.Rollback()
    panic(err)
}

var rid, rtitle string
for rows.Next() {
    if err = rows.Scan(&rid, &rtitle); err == nil {
        fmt.Println(rid, rtitle)
    }
}

if err = tx.Commit(); err != nil {
    panic(err)
}
```

## Middlewares
Specific sql functions support middleware handlers to be attached. These handlers
are executed before the sql functions and allow for extending their features.
Middleware handlers are attached to the database *DB object and they get inherited
when creating transactions, prepared statements and connections.

Each handler has the original context as well as a query context in its parameter
list. The query context contains details and arguments for calling the sql function.
The query context DOES NOT implement context.Context, and is not used for calling 
any underlying sql functions.

Handlers are attached on a selected list of sql functions.
```golang
target := []sqlm.Function{sqlm.FN_Query, sqlm.FN_Exec}
handler := func(ctx context.Context, qctx *sqlm.Context) {
    fmt.Println("Running Query:", qctx.Query, qctx.Args)
    qctx.Next()
    success := len(qctx.Errors()) == 0
    fmt.Println("Success:", success)
}

db.Use(handler, target)
```