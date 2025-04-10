package btree

import (
	"encoding/binary"
	"log"

	"github.com/TheTeemka/LitDB/dal"
)

type PageID = dal.PageID

type Item struct {
	Key   []byte
	Value []byte
}

type Node struct {
	*NodeDAL
	pageID     PageID
	items      []*Item
	childNodes []PageID
}

func NewEmptyNode() *Node {
	return &Node{}
}

func NewNode(items []*Item, childNodes []PageID, dal *dal.DAL) *Node {
	return &Node{
		pageID:     dal.GetNextPage(),
		NodeDAL:    NewNodeDAL(dal),
		items:      items,
		childNodes: childNodes,
	}
}

func NewItem(key, value []byte) *Item {
	return &Item{
		Key:   key,
		Value: value,
	}
}

func (n *Node) IsLeaf() bool {
	return len(n.childNodes) == 0
}

func (n *Node) Serialize(buf []byte) []byte {
	leftOff := 0
	rightOff := len(buf) - 1
	isLeaf := n.IsLeaf()
	if isLeaf {
		buf[leftOff] = 1
	}
	leftOff += 1

	binary.LittleEndian.PutUint16(buf[leftOff:], uint16(len(n.items)))
	leftOff += 2

	for i := 0; i < len(n.items); i++ {
		if !isLeaf {
			childNode := n.childNodes[i]
			binary.LittleEndian.PutUint64(buf[leftOff:], uint64(childNode))
			leftOff += 8
		}
		item := n.items[i]
		klen := len(item.Key)
		vlen := len(item.Value)

		rightOff -= vlen
		copy(buf[rightOff:], item.Value)

		rightOff -= 1
		buf[rightOff] = byte(vlen)

		rightOff -= klen
		copy(buf[rightOff:], item.Key)

		rightOff -= 1
		buf[rightOff] = byte(klen)

		offset := rightOff
		binary.LittleEndian.PutUint16(buf[leftOff:], uint16(offset))
		leftOff += 2
	}
	if !isLeaf {
		childNode := n.childNodes[len(n.childNodes)-1]
		binary.LittleEndian.PutUint64(buf[leftOff:], uint64(childNode))

		leftOff += 8
	}

	return buf
}

func (n *Node) Deserialize(buf []byte) {
	leftOff := 0

	isLeaf := buf[leftOff] == 1
	leftOff += 1

	itemCount := binary.LittleEndian.Uint16(buf[leftOff:])
	leftOff += 2

	for range itemCount {
		if !isLeaf {
			childNode := binary.LittleEndian.Uint64(buf[leftOff:])
			leftOff += 8
			n.childNodes = append(n.childNodes, PageID(childNode))
		}
		offset := binary.LittleEndian.Uint16(buf[leftOff:])
		leftOff += 2

		klen := uint16(buf[offset])
		offset += 1

		key := buf[offset : offset+klen]
		offset += klen

		vlen := uint16(buf[offset])
		offset += 1

		value := buf[offset : offset+vlen]
		offset += vlen

		n.items = append(n.items, NewItem(key, value))
	}

	if !isLeaf {
		childNode := binary.LittleEndian.Uint64(buf[leftOff:])
		leftOff += 8
		n.childNodes = append(n.childNodes, PageID(childNode))
	}
}

func (n *Node) elementSize(i int) int {
	size := 0
	size += len(n.items[i].Key)
	size += len(n.items[i].Value)
	size += 8 // childNode pageID
	size += 2 // offset uint16
	return size
}

func (n *Node) nodeSize() int {
	size := 0
	size += 1 // isLeaf byte
	size += 2 // item count uint16

	if n == nil {
		log.Println("Pizdes")
	}
	for i := range n.items {
		size += n.elementSize(i)
	}

	size += 8 // last childNode pageID uint64

	return size
}

func (n *Node) AddItem(item *Item, insertionIndex int) {
	if len(n.items) == insertionIndex {
		n.items = append(n.items, item)
	} else {
		n.items = append(n.items[:insertionIndex+1], n.items[insertionIndex:]...)
		n.items[insertionIndex] = item
	}
}

func (n *Node) AddChild(childPageID PageID, insertionIndex int) {
	if len(n.childNodes) == insertionIndex {
		n.childNodes = append(n.childNodes, childPageID)
	} else {
		n.childNodes = append(n.childNodes[:insertionIndex+1], n.childNodes[insertionIndex:]...)
		n.childNodes[insertionIndex] = childPageID
	}
}

func (n *Node) isOverPopulated() bool {
	return n.nodeSize() > n.dal.MaxTreshhold()
}

func (n *Node) isUnderPopulated() bool {
	return n.nodeSize() < n.dal.MinTreshhold()
}

func (n *Node) canSpareNode() bool {
	return !n.isUnderPopulated()
}
