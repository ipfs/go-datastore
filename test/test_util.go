package dstest

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base32"
	"errors"
	"testing"

	dstore "github.com/ipfs/go-datastore"
)

var ErrTest = errors.New("test error")

func RunBatchTest(t *testing.T, ds dstore.Batching) {
	ctx := t.Context()

	batch, err := ds.Batch(ctx)
	if err != nil {
		t.Fatal(err)
	}

	blocks := make([][]byte, 0, 20)
	keys := make([]dstore.Key, 0, 20)
	for range 20 {
		blk := make([]byte, 256*1024)
		rand.Read(blk)
		blocks = append(blocks, blk)

		key := dstore.NewKey(base32.StdEncoding.EncodeToString(blk[:8]))
		keys = append(keys, key)

		err = batch.Put(ctx, key, blk)
		if err != nil {
			t.Fatal(err)
		}
	}

	// Ensure they are not in the datastore before committing
	for _, k := range keys {
		_, err := ds.Get(ctx, k)
		if err == nil {
			t.Fatal("should not have found this block")
		}
	}

	// commit, write them to the datastore
	err = batch.Commit(ctx)
	if err != nil {
		t.Fatal(err)
	}

	for i, k := range keys {
		blk, err := ds.Get(ctx, k)
		if err != nil {
			t.Fatal(err)
		}

		if !bytes.Equal(blk, blocks[i]) {
			t.Fatal("blocks not correct!")
		}
	}
}

func RunBatchDeleteTest(t *testing.T, ds dstore.Batching) {
	ctx := t.Context()

	keys := make([]dstore.Key, 0, 20)
	for range 20 {
		blk := make([]byte, 16)
		rand.Read(blk)

		key := dstore.NewKey(base32.StdEncoding.EncodeToString(blk[:8]))
		keys = append(keys, key)

		err := ds.Put(ctx, key, blk)
		if err != nil {
			t.Fatal(err)
		}
	}

	batch, err := ds.Batch(ctx)
	if err != nil {
		t.Fatal(err)
	}

	for _, k := range keys {
		err := batch.Delete(ctx, k)
		if err != nil {
			t.Fatal(err)
		}
	}
	err = batch.Commit(ctx)
	if err != nil {
		t.Fatal(err)
	}

	for _, k := range keys {
		_, err := ds.Get(ctx, k)
		if err == nil {
			t.Fatal("shouldnt have found block")
		}
	}
}

func RunBatchPutAndDeleteTest(t *testing.T, ds dstore.Batching) {
	ctx := t.Context()

	batch, err := ds.Batch(ctx)
	if err != nil {
		t.Fatal(err)
	}

	ka := dstore.NewKey("/a")
	kb := dstore.NewKey("/b")

	if err = batch.Put(ctx, ka, []byte{1}); err != nil {
		t.Error(err)
	}
	if err = batch.Put(ctx, kb, []byte{2}); err != nil {
		t.Error(err)
	}
	if err = batch.Delete(ctx, ka); err != nil {
		t.Error(err)
	}
	if err = batch.Delete(ctx, kb); err != nil {
		t.Error(err)
	}
	if err = batch.Put(ctx, kb, []byte{3}); err != nil {
		t.Error(err)
	}

	// TODO: assert that nothing has been flushed yet? What are the semantics here?

	if err = batch.Commit(ctx); err != nil {
		t.Error(err)
	}

	_, err = ds.Get(ctx, ka)
	if err == nil {
		t.Errorf("expected to not find %s", ka)
	}
	if !errors.Is(err, dstore.ErrNotFound) {
		t.Error(err)
	}

	if v, err := ds.Get(ctx, kb); err != nil {
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

func (d *testDatastore) Check(_ context.Context) error {
	if d.testErrors {
		return ErrTest
	}
	return nil
}

func (d *testDatastore) Scrub(_ context.Context) error {
	if d.testErrors {
		return ErrTest
	}
	return nil
}

func (d *testDatastore) CollectGarbage(_ context.Context) error {
	if d.testErrors {
		return ErrTest
	}
	return nil
}
