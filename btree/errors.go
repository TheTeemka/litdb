package btree

import "errors"

var (
	ErrWriteInsideReadTxr = errors.New("can't perform a write operation inside a read transaction")
	ErrNotFound           = errors.New("resourse not found")
)
