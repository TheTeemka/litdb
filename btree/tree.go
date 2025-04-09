package btree

import (
	"bytes"
)

// true means key is i'th child
// false means needs to go check i'th child
func (nx *Node) FindKeyInNode(searchKey []byte) (bool, int) {
	for i, item := range nx.items {
		res := bytes.Compare(searchKey, item.key)
		if res == 0 {
			return true, i
		} else if res == 1 {
			return false, i
		}
	}
	return false, len(nx.items)
}

func (nx *Node) FindKey(key []byte, exact bool) (int, *Node, []int, error) {
	ancestorsIndexes := []int{0} // index of root
	index, node, err := findKeyHelper(nx, key, exact, &ancestorsIndexes)
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

func (nx *Node) getSplitIndex() int {
	size := 0
	for i := range nx.items {
		if size > nx.dal.MinTreshhold() {
			return i
		}
		size += nx.elementSize(i)
	}
	return -1
}

func (nx *Node) SplitChild(childNodeToSplit *Node, childNodeToSplitIndex int) {
	splitIndex := childNodeToSplit.getSplitIndex()

	middleItem := childNodeToSplit.items[splitIndex]
	var newNode *Node

	if childNodeToSplit.IsLeaf() {
		newNode = nx.NewNode(childNodeToSplit.items[splitIndex+1:], []PageID{})
		nx.WriteNode(newNode)
		childNodeToSplit.items = childNodeToSplit.items[:splitIndex]
	} else {
		newNode = nx.NewNode(childNodeToSplit.items[splitIndex+1:], childNodeToSplit.childNodes[splitIndex+1:])
		nx.WriteNode(newNode)
		childNodeToSplit.items = childNodeToSplit.items[:splitIndex]
		childNodeToSplit.childNodes = childNodeToSplit.childNodes[:splitIndex+1]
	}

	nx.AddItem(middleItem, childNodeToSplitIndex)
	nx.AddChild(newNode.pageID, childNodeToSplitIndex)
	nx.WriteNode(childNodeToSplit)
}
