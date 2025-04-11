package btree

import (
	"fmt"

	"github.com/TheTeemka/LitDB/internal/dal"
)

type NodeDAL struct {
	dal *dal.DAL
}

func NewNodeDAL(dal *dal.DAL) *NodeDAL {
	return &NodeDAL{dal}
}

func (nX *NodeDAL) ReadNode(pageID PageID) (*Node, error) {
	p, err := nX.dal.ReadPage(pageID)
	if err != nil {
		return nil, fmt.Errorf("reading node: %w", err)
	}
	node := NewEmptyNode()
	node.Deserialize(p.Data)
	node.pageID = pageID
	return node, nil
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

func (nX *NodeDAL) DeleteNode(pageID PageID) {
	dal := nX.dal
	dal.ReleasePage(pageID)
}
