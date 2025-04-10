package btree

import (
	"fmt"

	"github.com/TheTeemka/LitDB/dal"
)

type NodeDAL struct {
	dal *dal.DAL
}

func NewNodeDAL(dal *dal.DAL) *NodeDAL {
	return &NodeDAL{dal}
}

func readNode(dal *dal.DAL, pageID PageID) (*Node, error) {
	p, err := dal.ReadPage(pageID)
	if err != nil {
		return nil, fmt.Errorf("allocating empty page: %w", err)
	}
	node := NewEmptyNode()
	node.Deserialize(p.Data)
	node.pageID = pageID
	node.NodeDAL = NewNodeDAL(dal)
	return node, nil
}

func (nx *NodeDAL) NewNode(items []*Item, childNodes []PageID) *Node {
	return NewNode(items, childNodes, nx.dal)
}

func (nX *NodeDAL) ReadNode(pageID PageID) (*Node, error) {
	return readNode(nX.dal, pageID)
}

func (nX *NodeDAL) WriteNode(node *Node) error {
	dal := nX.dal

	p := dal.AllocateEmptyPage()
	if node.pageID == 0 {
		p.ID = dal.GetNextPage()
		node.pageID = p.ID
	} else {
		p.ID = node.pageID
	}

	p.Data = node.Serialize(p.Data)

	err := dal.WritePage(p)
	if err != nil {
		return fmt.Errorf("writing page: %w", err)
	}
	return nil
}

func (nX *NodeDAL) WriteNodes(nodes ...*Node) error {
	for _, node := range nodes {
		err := nX.WriteNode(node)
		if err != nil {
			return fmt.Errorf("writing nodes: %w", err)
		}
	}
	return nil
}

func (nX *NodeDAL) DeleteNode(node *Node) {
	dal := nX.dal
	dal.ReleasePage(node.pageID)
}

func (nX *NodeDAL) getAnsectorNodes(root *Node, ansectorIndex []int) ([]*Node, error) {
	ansectors := make([]*Node, len(ansectorIndex))
	ansectors[0] = root

	for i := 1; i < len(ansectorIndex); i++ {
		child, err := nX.ReadNode(ansectors[i-1].childNodes[ansectorIndex[i]])
		if err != nil {
			return nil, err
		}
		ansectors[i] = child
	}
	return ansectors, nil

}
