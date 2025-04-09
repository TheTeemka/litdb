package main

import (
	"fmt"
	"log"
	"os"

	"github.com/TheTeemka/LitDB/btree"
	"github.com/TheTeemka/LitDB/dal"
)

func main() {
	log.Default().SetFlags(log.Lshortfile)
	os.Remove("./mainTest")
	options := &dal.Options{
		PageSize:       64,
		MinFillPercent: 0.4,
		MaxFillPercent: 0.95,
	}
	dal, _ := dal.New("./mainTest", options)
	defer dal.Close()
	c := btree.NewCollection([]byte("collection1"), dal)

	err := c.Put([]byte("Key1"), []byte("Value1"))
	if err != nil {
		panic(err)
	}
	err = c.Put([]byte("Key2"), []byte("Value2"))
	if err != nil {
		panic(err)
	}
	err = c.Put([]byte("Key3"), []byte("Value3"))
	if err != nil {
		panic(err)
	}
	err = c.Put([]byte("Key4"), []byte("Value4"))
	if err != nil {
		panic(err)
	}
	item, err := c.Find([]byte("Key1"))
	if err != nil {
		panic(err)
	}

	fmt.Println(item)
	// time.Sleep(1000 * time.Second)
}
