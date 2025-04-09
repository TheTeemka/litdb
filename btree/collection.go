package btree

import (
	"bytes"
	"errors"

	"github.com/TheTeemka/LitDB/dal"
)

var (
	errNotFound = errors.New("item is not found")
)

type Collection struct {
	name   []byte
	rootID PageID

	dal *dal.DAL
}

func NewCollection(name []byte, dal *dal.DAL) *Collection {
	return &Collection{
		name:   name,
		dal:    dal,
		rootID: 0,
	}
}

func (c *Collection) Find(key []byte) (*Item, error) {
	root, err := readNode(c.dal, c.rootID)
	if err != nil {
		return nil, err
	}

	index, containingNode, _, err := root.FindKey(key, true)
	if err != nil {
		return nil, err
	}
	if index == -1 {
		return nil, errNotFound
	}
	return containingNode.items[index], nil
}

func (c *Collection) Put(key []byte, value []byte) error {
	item := NewItem(key, value)

	var root *Node
	var err error
	if c.rootID == 0 {
		root = NewNode([]*Item{item}, nil, c.dal)
		err = root.WriteNode(root)
		if err != nil {
			return err
		}
		c.rootID = root.pageID
		return nil
	}

	root, err = readNode(c.dal, c.rootID)
	if err != nil {
		return err
	}

	insertionIndex, nodeToInsertIn, ansectorIndex, err := root.FindKey(key, false)
	if err != nil {
		return err
	}

	if nodeToInsertIn.items != nil && bytes.Compare(nodeToInsertIn.items[insertionIndex].key, key) == 0 {
		nodeToInsertIn.items[insertionIndex] = item
	} else {
		nodeToInsertIn.AddItem(item, insertionIndex)
	}

	err = root.WriteNode(nodeToInsertIn)
	if err != nil {
		return err
	}

	ansectors := make([]*Node, len(ansectorIndex)+1)
	ansectors[0] = root
	for i := 1; i < len(ansectorIndex); i++ {
		child, err := root.ReadNode(ansectors[i-1].childNodes[ansectorIndex[i]])
		if err != nil {
			return err
		}
		ansectors[i] = child
	}

	for i := len(ansectorIndex) - 2; i >= 0; i-- {
		parentNode := ansectors[i+1]
		childNode := ansectors[i]
		if childNode.isOverPopulated() {
			parentNode.SplitChild(childNode, i)
		}
	}

	if root.isOverPopulated() {
		newRoot := root.NewNode([]*Item{}, []PageID{c.rootID})
		newRoot.SplitChild(root, 0)

		err := newRoot.WriteNode(newRoot)
		if err != nil {
			return err
		}

		c.rootID = newRoot.pageID
	}

	return nil

}
