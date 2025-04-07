package dal

import (
	"fmt"
	"os"
)

type pageID int64
type page struct {
	ID   pageID
	Data []byte
}

type DAL struct {
	file     *os.File
	pageSize int

	*freelist
	*meta
}

func New(path string) (*DAL, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, fmt.Errorf("error opening file for DAL: %v", err)
	}

	return &DAL{
		file:     file,
		pageSize: os.Getpagesize(),
		freelist: newFreeList(),
	}, nil
}

func (d *DAL) Close() error {
	if d.file != nil {
		err := d.file.Close()
		if err != nil {

			return fmt.Errorf("could not close file: %w", err)
		}
		d.file = nil
	}
	return nil
}

func (d *DAL) AllocateEmptyPage() *page {
	return &page{
		Data: make([]byte, d.pageSize),
	}
}

func (d *DAL) ReadPage(pgID pageID) (*page, error) {
	p := d.AllocateEmptyPage()
	p.ID = pgID

	offset := int(pgID) * d.pageSize
	_, err := d.file.ReadAt(p.Data, int64(offset))
	if err != nil {
		return nil, fmt.Errorf("reading page: %w", err)
	}
	return p, nil
}

func (d *DAL) WritePage(p *page) error {
	offset := int(p.ID) * d.pageSize
	_, err := d.file.WriteAt(p.Data, int64(offset))
	if err != nil {
		return fmt.Errorf("writing page: %w", err)
	}
	return nil
}

func (d *DAL) writeMeta(meta *meta) (*page, error) {
	p := d.AllocateEmptyPage()
	p.ID = metaPageID
	meta.serialize(p.Data)

	err := d.WritePage(p)
	if err != nil {
		return nil, fmt.Errorf("writing meta page: %w", err)
	}
	return p, nil
}

func (d *DAL) readMeta() (*meta, error) {
	p, err := d.ReadPage(metaPageID)
	if err != nil {
		return nil, fmt.Errorf("reading meta page: %w", err)
	}
	meta := newEmptyMeta()
	meta.deserialize(p.Data)
	return meta, nil
}
