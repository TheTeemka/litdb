package main

import (
	"log"
	"os"

	"github.com/TheTeemka/LitDB/internal/btree"
	"github.com/TheTeemka/LitDB/internal/dal"
)

func main() {
	log.Default().SetFlags(log.Lshortfile)

	os.Remove("./test.db")
	options := &dal.Options{
		PageSize:       (1 << 7),
		MinFillPercent: 0.4,
		MaxFillPercent: 0.95,
	}

	db, err := btree.Open("./test.db", options)
	if err != nil {
		panic(err)
	}

	c, err := db.GetCollection([]byte("Main Collection"))
	if err != nil {
		panic(err)
	}

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
		err := c.Put([]byte(item.key), []byte(item.val))
		if err != nil {
			panic(err)
		}
	}

	items = []struct {
		key string
		val string
	}{
		{key: "Key5", val: "Value5"},
		{key: "Key6", val: "Value6"},
		{key: "Key7", val: "Value7"},
		{key: "Key8", val: "Value8"},
	}

	for _, item := range items {
		err := c.Remove([]byte(item.key))
		if err != nil {
			panic(err)
		}
	}
}
