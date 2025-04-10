package btree

import (
	"bytes"
	"errors"
	"fmt"
	"log"

	"github.com/TheTeemka/LitDB/dal"
)

var (
	errNotFound = errors.New("item is not found")
)

type Collection struct {
	name   []byte
	rootID PageID

	nodeDal *NodeDAL
}

func NewCollection(name []byte, dal *dal.DAL) *Collection {
	return &Collection{
		name:    name,
		nodeDal: NewNodeDAL(dal),
		rootID:  0,
	}
}

func (c *Collection) Find(key []byte) (*Item, error) {
	root, err := c.getRootNode()
	fmt.Println(root.pageID)
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

	root, err := c.getRootNode()
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

	ansectors, err := c.nodeDal.getAnsectorNodes(root, ansectorIndex)

	for i := len(ansectorIndex) - 2; i >= 0; i-- {
		parentNode := ansectors[i+1]
		childNode := ansectors[i]
		if childNode.isOverPopulated() {
			parentNode.splitChild(childNode, i)
		}
	}

	if root.isOverPopulated() {
		newRoot := root.NewNode([]*Item{}, []PageID{c.rootID})
		newRoot.splitChild(root, 0)

		err := newRoot.WriteNode(newRoot)
		if err != nil {
			return err
		}

		c.rootID = newRoot.pageID
	}

	return nil

}

func (c *Collection) Remove(key []byte) error {
	rootNode, err := c.getRootNode()
	if err != nil {
		return err
	}

	removeItemIndex, nodeToRemoveFrom, ancestorsIndexes, err := rootNode.FindKey(key, true)
	if err != nil {
		return err
	}

	log.Println(len(ancestorsIndexes))
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

	log.Println("success")

	ancestors, err := rootNode.getAnsectorNodes(rootNode, ancestorsIndexes)
	if err != nil {
		return err
	}
	log.Println("success")

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

func (c *Collection) getRootNode() (*Node, error) {
	if c.rootID == 0 {
		return c.createRoot()
	}
	return c.nodeDal.ReadNode(c.rootID)
}

func (c *Collection) createRoot() (*Node, error) {
	root := c.nodeDal.NewNode(nil, nil)

	err := root.WriteNode(root)
	if err != nil {
		return nil, err
	}

	c.rootID = root.pageID
	return root, nil

}
