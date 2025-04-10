package btree

import (
	"bytes"
	"log"
)

// true means Key is i'th child
// false means needs to go check i'th child
func (n *Node) FindKeyInNode(searchKey []byte) (bool, int) {

	for i, item := range n.items {
		res := bytes.Compare(item.Key, searchKey)
		if res == 0 {
			return true, i
		} else if res == 1 {
			return false, i
		}
	}
	return false, len(n.items)
}

func (n *Node) FindKey(key []byte, exact bool) (int, *Node, []int, error) {
	ancestorsIndexes := []int{0} // index of root
	index, node, err := findKeyHelper(n, key, exact, &ancestorsIndexes)
	if err != nil {
		return -1, nil, nil, err
	}
	return index, node, ancestorsIndexes, nil
}

func findKeyHelper(node *Node, key []byte, exact bool, ansectors *[]int) (int, *Node, error) {
	wasFound, ind := node.FindKeyInNode(key)
	if wasFound {
		return ind, node, nil
	}

	if node.IsLeaf() {
		if exact {
			return -1, nil, errNotFound
		}
		return ind, node, nil
	}

	childNode, err := node.ReadNode(node.childNodes[ind])
	*ansectors = append(*ansectors, ind)
	if err != nil {
		return -1, nil, err
	}
	return findKeyHelper(childNode, key, exact, ansectors)
}

func (n *Node) getSplitIndex() int {
	size := 0
	for i := range n.items {
		log.Println(i, size)
		if size >= n.dal.MinTreshhold() {
			return i
		}
		size += n.elementSize(i)
	}
	return -1
}

// OldNode <- MiddleItem -> NewNode
func (n *Node) splitChild(childNodeToSplit *Node, childNodeToSplitIndex int) {
	splitIndex := childNodeToSplit.getSplitIndex()
	middleItem := childNodeToSplit.items[splitIndex]

	var newNode *Node

	if childNodeToSplit.IsLeaf() {
		newNode = n.NewNode(childNodeToSplit.items[splitIndex+1:], nil)
		childNodeToSplit.items = childNodeToSplit.items[:splitIndex]
	} else {
		newNode = n.NewNode(childNodeToSplit.items[splitIndex+1:], childNodeToSplit.childNodes[splitIndex+1:])
		childNodeToSplit.items = childNodeToSplit.items[:splitIndex]
		childNodeToSplit.childNodes = childNodeToSplit.childNodes[:splitIndex+1]
	}

	n.AddItem(middleItem, childNodeToSplitIndex)
	n.AddChild(newNode.pageID, childNodeToSplitIndex+1)
	n.WriteNodes(newNode, childNodeToSplit)
}

func (n *Node) removeItemFromLeaf(deletionIndex int) {
	n.items = append(n.items[:deletionIndex], n.items[deletionIndex+1:]...)
	n.WriteNode(n)
}

func (n *Node) removeItemFromInternal(deletionIndex int) ([]int, error) {
	affectedNodes := make([]int, 0)
	affectedNodes = append(affectedNodes, deletionIndex)

	// Starting from its left child, descend to the rightmost descendant.
	aNode, err := n.ReadNode(n.childNodes[deletionIndex])
	if err != nil {
		return nil, err
	}

	for !aNode.IsLeaf() {
		traversingIndex := len(aNode.childNodes) - 1
		aNode, err = n.ReadNode(aNode.childNodes[traversingIndex])
		if err != nil {
			return nil, err
		}
		affectedNodes = append(affectedNodes, traversingIndex)

	}

	n.items[deletionIndex] = aNode.items[len(aNode.items)-1]
	aNode.items = aNode.items[:len(aNode.items)-1]

	n.WriteNodes(n, aNode)

	return affectedNodes, nil
}

func (n *Node) rotateRight(aNode, pNode, bNode *Node, bNodeIndex int) {

	aNodeItem := aNode.items[len(aNode.items)-1]
	aNode.items = aNode.items[:len(aNode.items)-1]

	pNodeItemIndex := bNodeIndex - 1
	pNodeItem := pNode.items[pNodeItemIndex]
	pNode.items[pNodeItemIndex] = aNodeItem

	bNode.items = append([]*Item{pNodeItem}, bNode.items...)
	if !aNode.IsLeaf() {
		aNodeChild := aNode.childNodes[len(aNode.childNodes)-1]
		aNode.childNodes = aNode.childNodes[:len(aNode.childNodes)-1]
		bNode.childNodes = append([]PageID{aNodeChild}, bNode.childNodes...)

	}
}

// it does not write changes
func (n *Node) rotateLeft(aNode, pNode, bNode *Node, bNodeIndex int) {
	bNodeItem := bNode.items[0]
	bNode.items = bNode.items[1:]

	pNodeItemIndex := bNodeIndex - 1
	pNodeItem := pNode.items[pNodeItemIndex]
	pNode.items[pNodeItemIndex] = bNodeItem

	aNode.items = append(aNode.items, pNodeItem)
	if !bNode.IsLeaf() {
		bNodeChild := bNode.childNodes[0]
		bNode.childNodes = bNode.childNodes[1:]
		aNode.childNodes = append(aNode.childNodes, bNodeChild)

	}
}

// bNodeIndex must be bigge than 0
func (n *Node) merge(bNode *Node, bNodeIndex int) error {
	aNode, err := n.ReadNode(n.childNodes[bNodeIndex-1])
	if err != nil {
		return err
	}

	pNodeItem := n.items[bNodeIndex-1]
	n.items = append(n.items[:bNodeIndex-1], n.items[bNodeIndex:]...)

	aNode.items = append(aNode.items, pNodeItem)
	aNode.items = append(aNode.items, bNode.items...)
	n.childNodes = append(n.childNodes[:bNodeIndex], n.childNodes[bNodeIndex+1:]...)
	if !aNode.IsLeaf() {
		aNode.childNodes = append(aNode.childNodes, bNode.childNodes...)
	}

	return nil
}

func (n *Node) rebalanceRemove(unbalancedNode *Node, unbalancedNodeIndex int) error {
	pNode := n

	if len(pNode.childNodes)-1 != unbalancedNodeIndex {
		rightNodeIndex := unbalancedNodeIndex + 1
		rightNode, err := n.ReadNode(n.childNodes[rightNodeIndex])
		if err != nil {
			return err
		}
		if rightNode.canSpareNode() {
			n.rotateLeft(unbalancedNode, pNode, rightNode, rightNodeIndex)
			n.WriteNodes(unbalancedNode, pNode, rightNode)
			return nil
		}
	}

	if unbalancedNodeIndex != 0 {
		leftNodeIndex := unbalancedNodeIndex - 1
		leftNode, err := n.ReadNode(n.childNodes[leftNodeIndex])
		if err != nil {
			return err
		}
		if leftNode.canSpareNode() {
			n.rotateRight(leftNode, pNode, unbalancedNode, unbalancedNodeIndex)
			n.WriteNodes(leftNode, pNode, unbalancedNode)
			return nil
		}
	}

	if unbalancedNodeIndex == 0 {
		rightNode, err := n.ReadNode(n.childNodes[unbalancedNodeIndex+1])
		if err != nil {
			return err
		}

		return pNode.merge(rightNode, unbalancedNodeIndex+1)
	}

	return pNode.merge(unbalancedNode, unbalancedNodeIndex)
}
