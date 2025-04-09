package dal

import (
	"encoding/binary"
)

type freelist struct {
	countAllocatedPages PageID
	releasedPages       []PageID
}

func newFreeList() *freelist {
	return &freelist{
		countAllocatedPages: metaPageID,
		releasedPages:       []PageID{},
	}
}

func (fr *freelist) GetNextPage() PageID {
	if len(fr.releasedPages) != 0 {
		pgID := fr.releasedPages[len(fr.releasedPages)-1]
		fr.releasedPages = fr.releasedPages[:len(fr.releasedPages)-1]
		return pgID
	}
	fr.countAllocatedPages += 1
	return fr.countAllocatedPages
}

func (fr *freelist) ReleasePage(pgID PageID) {
	fr.releasedPages = append(fr.releasedPages, pgID)
}

func (fr *freelist) serialize(buf []byte) []byte {
	offset := 0

	binary.LittleEndian.PutUint64(buf[offset:], uint64(fr.countAllocatedPages))
	offset += 8

	binary.LittleEndian.PutUint64(buf[offset:], uint64(len(fr.releasedPages)))
	offset += 8

	for _, pageID := range fr.releasedPages {
		binary.LittleEndian.PutUint64(buf[offset:], uint64(pageID))
		offset += 8
	}

	return buf
}

func (fr *freelist) deserialize(buf []byte) {
	offset := 0

	fr.countAllocatedPages = PageID(binary.LittleEndian.Uint64(buf[offset:]))
	offset += 8

	cnt := binary.LittleEndian.Uint64(buf[offset:])
	offset += 8

	for range cnt {
		fr.releasedPages = append(fr.releasedPages, PageID(binary.LittleEndian.Uint64(buf[offset:])))
		offset += 8
	}

}
