package dal

import "encoding/binary"

type meta struct {
	rootPageID     PageID
	freelistPageID PageID
}

func newEmptyMeta() *meta {
	return &meta{}
}

func (m *meta) serialize(buf []byte) {
	off := 0

	binary.LittleEndian.PutUint64(buf[off:], uint64(m.freelistPageID))
	off += 8

	binary.LittleEndian.PutUint64(buf[off:], uint64(m.rootPageID))
	off += 8
}

func (m *meta) deserialize(buf []byte) {
	off := 0

	m.freelistPageID = PageID(binary.LittleEndian.Uint64(buf[off:]))
	off += 8

	m.rootPageID = PageID(binary.LittleEndian.Uint64(buf[off:]))
	off += 8

}
