package dal

import "encoding/binary"

type freelist struct {
	countAllocatedPages pageID
	releasedPages       []pageID
}

func newFreeList() *freelist {
	return &freelist{
		countAllocatedPages: metaPageID,
		releasedPages:       []pageID{},
	}
}

func (fr *freelist) GetNextPage() pageID {
	if len(fr.releasedPages) != 0 {
		pgID := fr.releasedPages[len(fr.releasedPages)-1]
		fr.releasedPages = fr.releasedPages[:len(fr.releasedPages)-1]
		return pgID
	}
	fr.countAllocatedPages += 1
	return fr.countAllocatedPages
}

func (fr *freelist) ReleasePage(pgID pageID) {
	fr.releasedPages = append(fr.releasedPages, pgID)
}

func (fr *freelist) serialize(buf []byte) []byte {
	offset := 0

	binary.LittleEndian.PutUint64(buf[offset:], uint64(fr.countAllocatedPages))
	offset += pageIDsize

	binary.LittleEndian.PutUint64(buf[offset:], uint64(len(fr.releasedPages)))
	offset += pageIDsize

	for _, pageID := range fr.releasedPages {
		binary.LittleEndian.PutUint64(buf[offset:], uint64(pageID))
		offset += 8
	}

	return buf
}

func (fr *freelist) deserialize(buf []byte) {
	offset := 0

	fr.countAllocatedPages = pageID(binary.LittleEndian.Uint64(buf[offset:]))
	offset += pageIDsize

	cnt := binary.LittleEndian.Uint64(buf[offset:])
	offset += pageIDsize

	for i := uint64(0); i < cnt; i++ {
		fr.releasedPages = append(fr.releasedPages, pageID(binary.LittleEndian.Uint64(buf[offset:])))
		offset += 8
	}

}
