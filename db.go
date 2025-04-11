package litdb

import (
	"github.com/TheTeemka/litdb/internal/btree"
	"github.com/TheTeemka/litdb/internal/dal"
)

type Options = dal.Options

func Open(path string, options *dal.Options) (*btree.DB, error) {
	return btree.Open(path, options)
}
