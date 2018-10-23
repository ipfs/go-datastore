package dstest

import (
	"bytes"
	"encoding/base32"
	"errors"
	"math/rand"
	"testing"

	dstore "github.com/ipfs/go-datastore"
)

var (
	TestError = errors.New("test error")
)

func RunBatchTest(t *testing.T, ds dstore.Batching) {
	batch, err := ds.Batch()
	if err != nil {
		t.Fatal(err)
	}

	var blocks [][]byte
	var keys []dstore.Key
	for i := 0; i < 20; i++ {
		blk := make([]byte, 256*1024)
		rand.Read(blk)
		blocks = append(blocks, blk)

		key := dstore.NewKey(base32.StdEncoding.EncodeToString(blk[:8]))
		keys = append(keys, key)

		err := batch.Put(key, blk)
		if err != nil {
			t.Fatal(err)
		}
	}

	// Ensure they are not in the datastore before comitting
	for _, k := range keys {
		_, err := ds.Get(k)
		if err == nil {
			t.Fatal("should not have found this block")
		}
	}

	// commit, write them to the datastore
	err = batch.Commit()
	if err != nil {
		t.Fatal(err)
	}

	for i, k := range keys {
		blk, err := ds.Get(k)
		if err != nil {
			t.Fatal(err)
		}

		if !bytes.Equal(blk, blocks[i]) {
			t.Fatal("blocks not correct!")
		}
	}
}

func RunBatchDeleteTest(t *testing.T, ds dstore.Batching) {
	var keys []dstore.Key
	for i := 0; i < 20; i++ {
		blk := make([]byte, 16)
		rand.Read(blk)

		key := dstore.NewKey(base32.StdEncoding.EncodeToString(blk[:8]))
		keys = append(keys, key)

		err := ds.Put(key, blk)
		if err != nil {
			t.Fatal(err)
		}
	}

	batch, err := ds.Batch()
	if err != nil {
		t.Fatal(err)
	}

	for _, k := range keys {
		err := batch.Delete(k)
		if err != nil {
			t.Fatal(err)
		}
	}
	err = batch.Commit()
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

func RunBatchPutAndDeleteTest(t *testing.T, ds dstore.Batching) {
	batch, err := ds.Batch()
	if err != nil {
		t.Fatal(err)
	}

	ka := dstore.NewKey("/a")
	kb := dstore.NewKey("/b")

	if err := batch.Put(ka, []byte{1}); err != nil {
		t.Error(err)
	}
	if err := batch.Put(kb, []byte{2}); err != nil {
		t.Error(err)
	}
	if err := batch.Delete(ka); err != nil {
		t.Error(err)
	}
	if err := batch.Delete(kb); err != nil {
		t.Error(err)
	}
	if err := batch.Put(kb, []byte{3}); err != nil {
		t.Error(err)
	}

	// TODO: assert that nothing has been flushed yet? What are the semantics here?

	if err := batch.Commit(); err != nil {
		t.Error(err)
	}

	switch _, err := ds.Get(ka); err {
	case dstore.ErrNotFound:
	case nil:
		t.Errorf("expected to not find %s", ka)
	default:
		t.Error(err)
	}

	if v, err := ds.Get(kb); err != nil {
		t.Error(err)
	} else {
		if len(v) != 1 || v[0] != 3 {
			t.Errorf("for key %s, expected %v, got %v", kb, []byte{3}, v)
		}
	}
}

type testDatastore struct {
	testErrors bool

	*dstore.MapDatastore
}

func NewTestDatastore(testErrors bool) *testDatastore {
	return &testDatastore{
		testErrors:   testErrors,
		MapDatastore: dstore.NewMapDatastore(),
	}
}

func (d *testDatastore) Check() error {
	if d.testErrors {
		return TestError
	}
	return nil
}

func (d *testDatastore) Scrub() error {
	if d.testErrors {
		return TestError
	}
	return nil
}

func (d *testDatastore) CollectGarbage() error {
	if d.testErrors {
		return TestError
	}
	return nil
}
