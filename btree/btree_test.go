package btree_test

import (
	"os"
	"testing"

	"github.com/TheTeemka/LitDB/btree"
	"github.com/TheTeemka/LitDB/dal"
)

func setupTestDB(t *testing.T) (*btree.Collection, func()) {
	t.Helper()
	os.Remove("test.db")

	options := &dal.Options{
		PageSize:       (1 << 7),
		MinFillPercent: 0.4,
		MaxFillPercent: 0.95,
	}

	db, err := btree.Open("test.db", options)
	if err != nil {
		t.Fatalf("Failed to create DAL: %v", err)
	}

	c, err := db.GetCollection([]byte("test_collection"))
	if err != nil {
		t.Fatal(err)
	}
	cleanup := func() {
		db.Close()
		// os.Remove("./test.db")
	}

	return c, cleanup
}

func TestTx(t *testing.T) {
	c, cleanup := setupTestDB(t)
	defer cleanup()

	tx := c.BeginTx()

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
		err := tx.Put([]byte(item.key), []byte(item.val))
		if err != nil {
			tx.Rollback()
			t.Fatal(err)
		}
	}

	items = []struct {
		key string
		val string
	}{
		{key: "Key5", val: "Value5"},
		{key: "Key6", val: "Value6"},
		{key: "Key7", val: "Value7"},
		{key: "Key9", val: "Value8"},
	}

	for _, item := range items {
		err := tx.Remove([]byte(item.key))
		if err != nil {
			tx.Rollback()
			t.Fatal(err)
		}
	}
	err := tx.Commit()
	if err != nil {
		t.Fatal(err)
	}
}

func TestBasicFeatures(t *testing.T) {
	c, cleanup := setupTestDB(t)
	defer cleanup()

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
