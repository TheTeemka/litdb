package main

import (
	"fmt"
	"log"
	"os"

	"github.com/TheTeemka/LitDB/btree"
	"github.com/TheTeemka/LitDB/dal"
	"github.com/fatih/color"
)

func main() {
	log.Default().SetFlags(log.Lshortfile)
	os.Remove("./mainTest")
	options := &dal.Options{
		PageSize:       (1 << 7),
		MinFillPercent: 0.4,
		MaxFillPercent: 0.95,
	}
	dal, _ := dal.New("./test.db", options)
	defer dal.Close()
	c := btree.NewCollection([]byte("collection1"), dal)

	items := []struct {
		key string
		val string
	}{
		{key: "Key1", val: "Value1"},
		{key: "Key2", val: "Value2"},
		{key: "Key3", val: "Value3"},
		{key: "Key4", val: "Value4"},
		{key: "Key5", val: "Value5"},
		{key: "Key6", val: "Value6"},
		{key: "Key7", val: "Value7"},
		{key: "Key8", val: "Value8"},
	}

	for _, item := range items {
		log.Println(color.YellowString("Next put"))
		err := c.Put([]byte(item.key), []byte(item.val))
		if err != nil {
			panic(err)
		}
	}

	item, err := c.Find([]byte("Key5"))
	if err != nil {
		panic(err)
	}
	fmt.Println(item)

	err = c.Remove([]byte("Key5"))
	if err != nil {
		panic(err)
	}

	err = c.Remove([]byte("Key6"))
	if err != nil {
		panic(err)
	}

	err = c.Remove([]byte("Key7"))
	if err != nil {
		panic(err)
	}
}
