package btree

import (
	"errors"
	"sync"

	"github.com/TheTeemka/litdb/internal/dal"
)

type DB struct {
	rwLock sync.RWMutex
	*NodeDAL
}

// in case of nil options, db will open with default options
func Open(path string, options *dal.Options) (*DB, error) {
	if options == nil {
		options = dal.DefaultOptions
	}
	dal, err := dal.New(path, options)
	if err != nil {
		return nil, err
	}

	db := &DB{
		sync.RWMutex{},
		NewNodeDAL(dal),
	}

	return db, nil
}

func (db *DB) Close() error {
	return db.dal.Close()
}

func (db *DB) BeginTx(coll *Collection) *tx {
	return newTx(db, coll, false)
}

func (db *DB) getRootCollection() *Collection {
	rootCollection := newCollection(nil, db)
	rootCollection.rootID = db.dal.CollectionRootID()
	if rootCollection.rootID == 0 {
		panic("rootCollectionID must not be zero")
	}
	return rootCollection
}

func (db *DB) GetCollection(name []byte) (*Collection, error) {
	rootCollection := db.getRootCollection()
	item, err := rootCollection.Find(name)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return nil, err
	}

	var c *Collection
	if item == nil {
		c = newCollection(name, db)
		c.rootID = db.dal.GetNextPage()
		rootCollection.putCollection(c)
	} else {
		c = newCollection(name, db)
		c.deserialize(item.Value)
	}
	return c, nil
}
