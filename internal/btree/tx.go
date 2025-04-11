package btree

import (
	"bytes"
	"fmt"
	"log"
)

type tx struct {
	dirtyNodes        map[PageID]*Node
	pagesToDelete     []PageID
	coll              *Collection
	allocatedPageNums []PageID

	write bool

	db *DB
}

func newTx(db *DB, coll *Collection, write bool) *tx {
	return &tx{
		dirtyNodes:        map[PageID]*Node{},
		pagesToDelete:     nil,
		allocatedPageNums: nil,
		write:             write,
		db:                db,
		coll:              coll,
	}
}

func (tx *tx) Wlock() {
	tx.db.rwLock.Lock()
	tx.write = true
}

func (tx *tx) Wunlock() {
	tx.db.rwLock.Unlock()
	tx.write = false
}

func (tx *tx) Rlock() {
	tx.db.rwLock.RLock()
	tx.write = false
}

func (tx *tx) Runlock() {
	tx.db.rwLock.RUnlock()
	tx.write = false
}

func (tx *tx) ReadNode(pageNum PageID) (*Node, error) {
	if node, ok := tx.dirtyNodes[pageNum]; ok {
		return node, nil
	}

	node, err := tx.db.NodeDAL.ReadNode(pageNum)
	if err != nil {
		return nil, err
	}
	node.tx = tx
	return node, nil
}
func (tx *tx) WriteNode(node *Node) error {
	tx.dirtyNodes[node.pageID] = node
	node.tx = tx
	return nil
}

func (tx *tx) WriteNodes(nodes ...*Node) error {
	for _, node := range nodes {
		tx.WriteNode(node)
	}
	return nil
}

func (tx *tx) deleteNode(pageID PageID) {
	tx.pagesToDelete = append(tx.pagesToDelete, pageID)
}

func (tx *tx) NewNode(items []*Item, childNodes []PageID) *Node {
	return NewNode(items, childNodes, tx)
}

func (tx *tx) Rollback() {
	tx.dirtyNodes = nil
	tx.pagesToDelete = nil
	for _, pageNum := range tx.allocatedPageNums {
		tx.db.NodeDAL.dal.ReleasePage(pageNum)
	}
}

func (tx *tx) Commit() error {
	for _, node := range tx.dirtyNodes {
		err := tx.db.WriteNode(node)
		if err != nil {
			return err
		}
	}

	for _, pageID := range tx.pagesToDelete {
		tx.db.DeleteNode(pageID)
	}

	err := tx.db.dal.WriteFreeList()
	if err != nil {
		return nil
	}

	tx.dirtyNodes = nil
	tx.pagesToDelete = nil
	tx.allocatedPageNums = nil
	return nil
}

func (tx *tx) Find(key []byte) (*Item, error) {
	tx.Rlock()
	defer tx.Runlock()

	root, err := tx.getRootNode()
	fmt.Println(root)
	if err != nil {
		return nil, err
	}

	index, containingNode, _, err := root.FindKey(key, true)
	if err != nil {
		return nil, err
	}
	if index == -1 {
		return nil, ErrNotFound
	}
	return containingNode.items[index], nil
}

func (tx *tx) Put(key []byte, value []byte) error {
	tx.Wlock()
	defer tx.Wunlock()
	item := NewItem(key, value)

	root, err := tx.getRootNode()
	if err != nil {
		return err
	}

	insertionIndex, nodeToInsertIn, ansectorIndex, err := root.FindKey(key, false)
	if err != nil {
		return err
	}

	if nodeToInsertIn.items != nil && insertionIndex < len(nodeToInsertIn.items) &&
		bytes.Compare(nodeToInsertIn.items[insertionIndex].Key, key) == 0 {
		nodeToInsertIn.items[insertionIndex] = item
	} else {
		nodeToInsertIn.AddItem(item, insertionIndex)
	}

	err = root.WriteNode(nodeToInsertIn)
	if err != nil {
		return err
	}

	ansectors, err := root.getAnsectorNodes(root, ansectorIndex)

	for i := len(ansectorIndex) - 2; i >= 0; i-- {
		parentNode := ansectors[i+1]
		childNode := ansectors[i]
		if childNode.isOverPopulated() {
			parentNode.splitChild(childNode, i)
		}
	}

	if root.isOverPopulated() {
		newRoot := root.NewNode([]*Item{}, []PageID{tx.coll.rootID})
		newRoot.splitChild(root, 0)

		err := newRoot.WriteNode(newRoot)
		if err != nil {
			return err
		}

		tx.coll.rootID = newRoot.pageID
	}

	return nil

}

func (tx *tx) Remove(key []byte) error {
	tx.Wlock()
	defer tx.Wunlock()
	rootNode, err := tx.getRootNode()
	if err != nil {
		return err
	}

	removeItemIndex, nodeToRemoveFrom, ancestorsIndexes, err := rootNode.FindKey(key, true)
	if err != nil {
		return err
	}

	if nodeToRemoveFrom.IsLeaf() {
		nodeToRemoveFrom.removeItemFromLeaf(removeItemIndex)
	} else {
		affectedNodes, err := nodeToRemoveFrom.removeItemFromInternal(removeItemIndex)
		if err != nil {
			return nil
		}
		log.Println(len(affectedNodes))

		ancestorsIndexes = append(ancestorsIndexes, affectedNodes...)
	}

	ancestors, err := rootNode.getAnsectorNodes(rootNode, ancestorsIndexes)
	if err != nil {
		return err
	}

	for i := len(ancestors) - 2; i >= 0; i-- {
		pNode := ancestors[i]
		cNode := ancestors[i+1]
		if cNode.isUnderPopulated() {
			err = pNode.rebalanceRemove(cNode, ancestorsIndexes[i+1])
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (tx *tx) getRootNode() (*Node, error) {
	n, err := tx.ReadNode(tx.coll.rootID)
	if err != nil {
		return nil, err
	}
	n.tx = tx
	return n, nil
}
