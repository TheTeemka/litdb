package dal

import "encoding/binary"

type meta struct {
	collectionRootPageID PageID
	freelistPageID       PageID
}

func newEmptyMeta() *meta {
	return &meta{}
}

func (m *meta) serialize(buf []byte) {
	off := 0
	buf[off] = 'm'
	off += 1
	binary.LittleEndian.PutUint64(buf[off:], uint64(m.freelistPageID))
	off += 8

	binary.LittleEndian.PutUint64(buf[off:], uint64(m.collectionRootPageID))
	off += 8
}

func (m *meta) deserialize(buf []byte) {
	off := 0
	if buf[off] != 'm' {
		panic("there is no metapage starting with m")
	}
	off += 1

	m.freelistPageID = PageID(binary.LittleEndian.Uint64(buf[off:]))
	off += 8

	m.collectionRootPageID = PageID(binary.LittleEndian.Uint64(buf[off:]))
	off += 8

}
