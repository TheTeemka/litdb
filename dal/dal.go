package dal

import (
	"errors"
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

	meta *meta
	*freelist
}

func New(path string) (*DAL, error) {
	dal := &DAL{
		pageSize: os.Getpagesize(),
	}

	if _, err := os.Stat(path); err == nil { // there is no error
		dal.file, err = os.OpenFile(path, os.O_RDWR, 0)
		if err != nil {
			return nil, fmt.Errorf("could not open file: %w", err)
		}

		err := dal.readMeta()
		if err != nil {
			return nil, fmt.Errorf("could not read meta: %w", err)
		}

		err = dal.readFreeList()
		if err != nil {
			return nil, fmt.Errorf("could not read freelist: %w", err)
		}
	} else if errors.Is(err, os.ErrNotExist) {
		dal.file, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			return nil, fmt.Errorf("could not create file: %w", err)
		}

		dal.meta = newEmptyMeta()
		dal.freelist = newFreeList()
		dal.meta.freelistPageID = dal.GetNextPage()
		err := dal.writeFreeList()
		if err != nil {
			return nil, fmt.Errorf("could not write freelist: %w", err)
		}
	} else {
		return nil, err
	}

	return dal, nil
}

func (d *DAL) Close() error {
	d.writeMeta()
	d.writeFreeList()
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

func (d *DAL) writeMeta() (*page, error) {
	p := d.AllocateEmptyPage()
	p.ID = metaPageID
	d.meta.serialize(p.Data)

	err := d.WritePage(p)
	if err != nil {
		return nil, fmt.Errorf("writing meta page: %w", err)
	}
	return p, nil
}

func (d *DAL) readMeta() error {
	p, err := d.ReadPage(metaPageID)
	if err != nil {
		return fmt.Errorf("reading meta page: %w", err)
	}
	meta := newEmptyMeta()
	meta.deserialize(p.Data)
	d.meta = meta
	return nil
}

func (d *DAL) writeFreeList() error {
	p := d.AllocateEmptyPage()
	p.ID = d.meta.freelistPageID
	d.freelist.serialize(p.Data)

	err := d.WritePage(p)
	if err != nil {
		return fmt.Errorf("writing freelist page: %w", err)
	}
	return nil
}

func (d *DAL) readFreeList() error {
	p, err := d.ReadPage(d.meta.freelistPageID)
	if err != nil {
		return fmt.Errorf("reading freelist page: %w", err)
	}
	freelist := newFreeList()
	freelist.deserialize(p.Data)
	d.freelist = freelist
	return nil
}
