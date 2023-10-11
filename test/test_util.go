package dstest

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base32"
	"errors"
	"testing"

	dstore "github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
)

var ErrTest = errors.New("test error")

func RunBatchTest(t *testing.T, ds dstore.Batching) {
	ctx := context.Background()

	batch, err := ds.Batch(ctx)
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

		err := batch.Put(ctx, key, blk)
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
	ctx := context.Background()

	var keys []dstore.Key
	for i := 0; i < 20; i++ {
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
	ctx := context.Background()

	batch, err := ds.Batch(ctx)
	if err != nil {
		t.Fatal(err)
	}

	ka := dstore.NewKey("/a")
	kb := dstore.NewKey("/b")

	if err := batch.Put(ctx, ka, []byte{1}); err != nil {
		t.Error(err)
	}
	if err := batch.Put(ctx, kb, []byte{2}); err != nil {
		t.Error(err)
	}
	if err := batch.Delete(ctx, ka); err != nil {
		t.Error(err)
	}
	if err := batch.Delete(ctx, kb); err != nil {
		t.Error(err)
	}
	if err := batch.Put(ctx, kb, []byte{3}); err != nil {
		t.Error(err)
	}

	// TODO: assert that nothing has been flushed yet? What are the semantics here?

	if err := batch.Commit(ctx); err != nil {
		t.Error(err)
	}

	switch _, err := ds.Get(ctx, ka); err {
	case dstore.ErrNotFound:
	case nil:
		t.Errorf("expected to not find %s", ka)
	default:
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

var _ dstore.TxnDatastore = (*testTxnDatastore)(nil)

type testTxnDatastore struct {
	testErrors bool

	*dstore.MapDatastore
}

func NewTestTxnDatastore(ms *dstore.MapDatastore, testErrors bool) *testTxnDatastore {
	if ms == nil {
		ms = dstore.NewMapDatastore()
	}
	return &testTxnDatastore{
		testErrors:   testErrors,
		MapDatastore: ms,
	}
}

func (t *testTxnDatastore) Check(_ context.Context) error {
	if t.testErrors {
		return ErrTest
	}
	return nil
}

func (t *testTxnDatastore) Scrub(_ context.Context) error {
	if t.testErrors {
		return ErrTest
	}
	return nil
}

func (t *testTxnDatastore) CollectGarbage(_ context.Context) error {
	if t.testErrors {
		return ErrTest
	}
	return nil
}

func (t *testTxnDatastore) NewTransaction(ctx context.Context, readOnly bool) (dstore.Txn, error) {
	return newTestTx(t.testErrors, t.MapDatastore), nil
}

var _ dstore.Txn = (*testTxn)(nil)

type testTxn struct {
	dirty     map[dstore.Key][]byte
	committed *dstore.MapDatastore
}

func newTestTx(testTxErrors bool, committed *dstore.MapDatastore) *testTxn {
	return &testTxn{
		dirty:     make(map[dstore.Key][]byte),
		committed: committed,
	}
}

// It is unclear from the dstore.Txn interface definition whether reads should happen from the dirty or committed or both
// It says that operations will not be applied until Commit() is called, but this doesn't really make sense for the Read
// operations as their interface is not designed for returning results asynchronously (except Query).
// For this test datastore, we simply Read from both dirty and committed entries with dirty values overshadowing committed values.

// NOTE: looking at go-ds-badger2, it looks like Get, Has, and GetSize only read from the dirty (uncommitted badger txn),
// whereas Query considers both the dirty transaction and the underlying committed datastore.

func (t *testTxn) Get(ctx context.Context, key dstore.Key) ([]byte, error) {
	if val, ok := t.dirty[key]; ok {
		return val, nil
	}
	return t.committed.Get(ctx, key)
}

func (t *testTxn) Has(ctx context.Context, key dstore.Key) (bool, error) {
	if _, ok := t.dirty[key]; ok {
		return true, nil
	}

	return t.committed.Has(ctx, key)
}

func (t *testTxn) GetSize(ctx context.Context, key dstore.Key) (int, error) {
	if val, ok := t.dirty[key]; ok {
		return len(val), nil
	}

	return t.committed.GetSize(ctx, key)
}

func (t *testTxn) Query(ctx context.Context, q dsq.Query) (dsq.Results, error) {
	// not entirely sure if Query is *supposed* to access both uncommitted and committed data, but if so I think this
	// is the simplest way of handling it and the overhead should be fine for testing purposes
	transientStore := dstore.NewMapDatastore()
	transientBatch, err := transientStore.Batch(ctx)
	if err != nil {
		return nil, err
	}

	// move committed results into the transientStore
	committedResults, err := t.committed.Query(ctx, q)
	if err != nil {
		return nil, err
	}
	defer func() {
		committedResults.Close()
	}()

	for {
		res, ok := committedResults.NextSync()
		if !ok {
			break
		}
		if res.Error != nil {
			return nil, res.Error
		}
		key := dstore.RawKey(res.Key)
		if err := transientBatch.Put(ctx, key, res.Value); err != nil {
			return nil, err
		}
	}
	// overwrite transientStore with the dirty results so we can query the union of them
	for k, v := range t.dirty {
		if err := transientBatch.Put(ctx, k, v); err != nil {
			return nil, err
		}
	}

	// commit the transientStore batch
	if err := transientBatch.Commit(ctx); err != nil {
		return nil, err
	}

	// apply the query to the transient store, return its results
	return transientStore.Query(ctx, q)
}

func (t *testTxn) Put(ctx context.Context, key dstore.Key, value []byte) error {
	t.dirty[key] = value
	return nil
}

func (t *testTxn) Delete(ctx context.Context, key dstore.Key) error {
	delete(t.dirty, key)
	return t.committed.Delete(ctx, key)
}

func (t *testTxn) Commit(ctx context.Context) error {
	batch, err := t.committed.Batch(ctx)
	if err != nil {
		return err
	}
	for k, v := range t.dirty {
		if err := batch.Put(ctx, k, v); err != nil {
			return err
		}
	}
	return batch.Commit(ctx)
}

func (t *testTxn) Discard(ctx context.Context) {
	t.dirty = make(map[dstore.Key][]byte)
}
