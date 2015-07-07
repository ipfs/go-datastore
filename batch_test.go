package datastore

import (
	"bytes"
	"encoding/base32"
	"testing"

	rand "github.com/jbenet/go-datastore/Godeps/_workspace/src/github.com/dustin/randbo"
)

func TestBasicBatch(t *testing.T) {
	ds := NewMapDatastore()
	batch := NewBasicBatch(ds)

	r := rand.New()
	var blocks [][]byte
	var keys []Key
	for i := 0; i < 20; i++ {
		blk := make([]byte, 256*1024)
		r.Read(blk)
		blocks = append(blocks, blk)

		key := NewKey(base32.StdEncoding.EncodeToString(blk[:8]))
		keys = append(keys, key)

		err := batch.Put(key, blk)
		if err != nil {
			t.Fatal(err)
		}
	}
	err := batch.Commit()
	if err != nil {
		t.Fatal(err)
	}

	for i, k := range keys {
		blk, err := ds.Get(k)
		if err != nil {
			t.Fatal(err)
		}

		if !bytes.Equal(blk.([]byte), blocks[i]) {
			t.Fatal("blocks not correct!")
		}
	}
}

func TestBasicBatchDelete(t *testing.T) {
	ds := NewMapDatastore()

	r := rand.New()
	var keys []Key
	for i := 0; i < 20; i++ {
		blk := make([]byte, 16)
		r.Read(blk)

		key := NewKey(base32.StdEncoding.EncodeToString(blk[:8]))
		keys = append(keys, key)

		err := ds.Put(key, blk)
		if err != nil {
			t.Fatal(err)
		}
	}

	batch := NewBasicBatch(ds)
	for _, k := range keys {
		err := batch.Delete(k)
		if err != nil {
			t.Fatal(err)
		}
	}
	err := batch.Commit()
	if err != nil {
		t.Fatal(err)
	}

	for _, k := range keys {
		_, err := ds.Get(k)
		if err == nil {
			t.Fatal("shouldnt have found block")
		}
	}
}
