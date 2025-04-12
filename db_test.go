package litdb_test

import (
	"log"
	"testing"

	"github.com/TheTeemka/litdb"
	"github.com/TheTeemka/litdb/internal/btree"
)

func init() {
	log.Default().SetFlags(log.Lshortfile)
}
func setupTestDB(t *testing.T) (*btree.Collection, func()) {
	t.Helper()
	// os.Remove("test.db")
	options := &litdb.Options{
		PageSize:       (1 << 7),
		MinFillPercent: 0.4,
		MaxFillPercent: 0.95,
	}

	db, err := litdb.Open("test.db", options)
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
	tx := c.BeginTX()

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
		{key: "Key8", val: "Value8"},
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
	cleanup()

	c, cleanup = setupTestDB(t)

	// _, err := c.Find([]byte("Key1"))
	// if err != nil {
	// 	t.Error(err)
	// }
	cleanup()
}
