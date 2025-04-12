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

func (c *Collection) BeginTX() *tx {
	return newTx(c.db, c.rootID, false)
}

func (c *Collection) Find(key []byte) (*Item, error) {
	tx := c.BeginTX()
	item, err := tx.Find(key)
	if err != nil {
		return nil, err
	}
	tx.Commit()
	if c.rootID != tx.rootPageID {
		c.Save(tx.rootPageID)
	}
	return item, nil
}

func (c *Collection) Put(key []byte, val []byte) error {
	tx := c.BeginTX()
	err := tx.Put(key, val)
	if err != nil {
		return err
	}
	tx.Commit()
	if c.rootID != tx.rootPageID {
		c.Save(tx.rootPageID)
	}
	return nil
}

func (c *Collection) Remove(key []byte) error {
	tx := c.BeginTX()
	err := tx.Remove(key)
	if err != nil {
		return err
	}
	tx.Commit()
	if c.rootID != tx.rootPageID {
		c.Save(tx.rootPageID)
	}
	return nil
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

func (c *Collection) Save(newRootPageID PageID) error {
	if c.db.dal.CollectionRootID() == c.rootID { //c is root collection
		c.rootID = newRootPageID
		c.db.dal.UpdateCollectionRootID(newRootPageID)
	} else {
		rc := c.db.getRootCollection()

		c.rootID = newRootPageID
		rc.putCollection(c)
	}
	return c.db.putCollection(c)
}

func (rc *Collection) putCollection(c *Collection) error {
	value := make([]byte, 8)
	c.serialize(value)

	return rc.Put(c.name, value)
}
