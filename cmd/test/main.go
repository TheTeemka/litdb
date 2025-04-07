package main

import (
	"log"

	"github.com/TheTeemka/LitDB/dal"
)

func main() {
	log.Default().SetFlags(log.Ltime | log.Lshortfile)
	d, err := dal.New("db.db")
	if err != nil {
		panic(err)
	}

	p := d.AllocateEmptyPage()
	p.ID = d.GetNextPage()
	copy(p.Data[:], "data")

	// commit it
	_ = d.WritePage(p)
	d.Close()

}
