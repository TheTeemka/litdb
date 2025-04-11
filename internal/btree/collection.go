package btree

import (
	"encoding/binary"
)

type Collection struct {
	name   []byte
	rootID PageID
	db     *DB
}

func newCollection(name []byte, db *DB) *Collection {
	return &Collection{
		name: name,
		db:   db,
	}
}

func (c *Collection) Find(key []byte) (*Item, error) {
	tx := c.db.BeginTx(c)
	item, err := tx.Find(key)
	if err != nil {
		return nil, err
	}
	tx.Commit()
	return item, nil
}

func (c *Collection) Put(key []byte, val []byte) error {
	tx := c.db.BeginTx(c)
	err := tx.Put(key, val)
	if err != nil {
		return err
	}
	tx.Commit()
	return nil
}

func (c *Collection) Remove(key []byte) error {
	tx := c.db.BeginTx(c)
	err := tx.Remove(key)
	if err != nil {
		return err
	}
	tx.Commit()
	return nil
}

func (c *Collection) BeginTx() *tx {
	return c.db.BeginTx(c)
}

func (c *Collection) serialize(buf []byte) {
	off := 0
	binary.LittleEndian.PutUint64(buf[off:], uint64(c.rootID))
	off += 8
}

func (c *Collection) deserialize(buf []byte) {
	off := 0
	c.rootID = PageID(binary.LittleEndian.Uint64(buf[off:]))
	off += 8
}

func (rc *Collection) putCollection(c *Collection) error {
	value := make([]byte, 8)
	c.serialize(value)

	return rc.Put(c.name, value)
}
