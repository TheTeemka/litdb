package dal

import "encoding/binary"

type meta struct {
	freelistPageID pageID
}

func newEmptyMeta() *meta {
	return &meta{}
}

func (m *meta) serialize(buf []byte) {
	binary.LittleEndian.PutUint64(buf, uint64(m.freelistPageID))
}

func (m *meta) deserialize(buf []byte) {
	m.freelistPageID = pageID(binary.LittleEndian.Uint64(buf))

}
