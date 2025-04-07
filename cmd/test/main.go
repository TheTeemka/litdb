package main

import (
	"fmt"
	"os"

	"github.com/TheTeemka/LitDB/dal"
)

func main() {
	dal, _ := dal.New("db.db")

	// create a new page
	p := dal.AllocateEmptyPage()
	p.ID = dal.GetNextPage()
	copy(p.Data[:], "data")

	// commit it
	_ = dal.WritePage(p)
	fmt.Println(os.Getpagesize())
}
