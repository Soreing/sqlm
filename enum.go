package sqlm

type Function int

const (
	FN_Begin Function = iota
	FN_Commit
	FN_Rollback
	FN_Exec
	FN_Ping
	FN_Prepare
	FN_Query
)

type Source int

const (
	SRC_Database Source = iota
	SRC_Transaction
	SRC_Statement
	SRC_Connection
)
