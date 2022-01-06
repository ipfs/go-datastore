package lrustore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	ds "github.com/ipfs/go-datastore"
	fs "github.com/ipfs/go-datastore/failstore"
	dsq "github.com/ipfs/go-datastore/query"
)

const testCapacity = 5

func TestLRUStore(t *testing.T) {
	dstore := ds.NewMapDatastore()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	lruStore, err := New(ctx, dstore, testCapacity)
	if err != nil {
		t.Fatal(err)
	}

	key := ds.NewKey("hw")
	val := []byte("hello world")

	// Test Put and Get.
	err = lruStore.Put(ctx, key, val)
	if err != nil {
		t.Fatal(err)
	}
	val2, err := lruStore.Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(val2, val) {
		t.Fatal("wrong value returned")
	}

	// Test LRUStore eviction.
	for i := 0; i < lruStore.Cap(); i++ {
		k := ds.NewKey(fmt.Sprintf("key-%d", i))
		err = lruStore.Put(ctx, k, []byte(fmt.Sprintf("val-%d", i)))
		if err != nil {
			t.Fatal(err)
		}
	}
	if lruStore.Len() != lruStore.Cap() {
		t.Fatalf("expected len to be %d, got %d", lruStore.Cap(), lruStore.Len())
	}
	_, err = lruStore.Get(ctx, key)
	if err != ds.ErrNotFound {
		t.Fatalf("Expected error %s, got %s", ds.ErrNotFound, err)
	}
	_, err = lruStore.dstore.Get(ctx, key)
	if err != ds.ErrNotFound {
		t.Fatalf("value for %q was not removed from datastore", key)
	}

	// Check that key-0 is able to be retrieved
	key0 := ds.NewKey("key-0")
	val, err = lruStore.Get(ctx, key0)
	if err != nil {
		t.Fatalf("Failed to get key-0: %s", err)
	}
	ok, err := lruStore.Has(ctx, key0)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("LRUStore should have key-0")
	}
	if !bytes.Equal(val, []byte("val-0")) {
		t.Fatalf("Wrong value for key-0")
	}
	sz, err := lruStore.GetSize(ctx, key0)
	if err != nil {
		t.Fatal(err)
	}
	if sz != len(val) {
		t.Fatalf("GetSize returned wrong size for key-0, expected %d, got %d", len(val), sz)
	}

	// Test loading LRUStore from datastore.
	lruStore, err = New(ctx, dstore, testCapacity-2)
	if err != nil {
		t.Fatal(err)
	}
	// Check that LRUStore was resized.
	if lruStore.Cap() != testCapacity {
		t.Fatalf("LRUStore did not resize to %d", testCapacity)
	}
	if lruStore.Len() != lruStore.Cap() {
		t.Fatalf("expected %d items in LRUStore, got %d", lruStore.Cap(), lruStore.Len())
	}

	// Check that key0 is still present, and make is newest in LRU.
	_, err = lruStore.Get(ctx, key0)
	if err != nil {
		t.Fatalf("Failed to get key-0: %s", err)
	}

	// Resize LRUStore to something smaller.
	newCap := lruStore.Len() - 2
	toEvict := lruStore.Len() - newCap
	evicted, err := lruStore.Resize(ctx, newCap)
	if err != nil {
		t.Fatal(err)
	}
	if evicted != toEvict {
		t.Fatalf("expected %d items to be evicted, got %d", toEvict, evicted)
	}

	// Delete key from LRUStore and varify it gets deleted from datastore.
	prevLen := lruStore.Len()
	err = lruStore.Delete(ctx, key0)
	if err != nil {
		t.Fatal(err)
	}
	if lruStore.Len() != prevLen-1 {
		t.Fatalf("expected %d items in LRUStore, got %d", prevLen-1, lruStore.Len())
	}
	_, err = lruStore.Get(ctx, key0)
	if err != ds.ErrNotFound {
		t.Fatalf("Expected error %s, got %s", ds.ErrNotFound, err)
	}
	_, err = lruStore.dstore.Get(ctx, key0)
	if err != ds.ErrNotFound {
		t.Fatalf("value for %q was not removed from datastore", key0)
	}
	ok, err = lruStore.Has(ctx, key0)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("LRUStore should not have key-0 after Delete")
	}

	// Test resize larger
	evicted, err = lruStore.Resize(ctx, lruStore.Cap()*2)
	if err != nil {
		t.Fatal(err)
	}
	if evicted != 0 {
		t.Fatal("increasing capacity should not evict entries")
	}

	// Test LRUStore Clear.
	lruStore.Clear(ctx)
	if lruStore.Len() != 0 {
		t.Fatal("LRUStore was not purged")
	}
	// Check that no keys are loaded from datastore.
	lruStore, err = New(ctx, dstore, testCapacity)
	if err != nil {
		t.Fatal(err)
	}
	if lruStore.Len() != 0 {
		t.Fatal("LRUStore was not purged")
	}

	du, err := lruStore.DiskUsage(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if du != 0 {
		t.Fatal("DiskUsage should be 0")
	}

	b, err := lruStore.Batch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if b == nil {
		t.Fatal("nil Batching interface")
	}

	err = lruStore.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestEvictionFail(t *testing.T) {
	errFailed := errors.New("failed")
	dstore := fs.NewFailstore(ds.NewMapDatastore(), func(op string) error {
		if op == "delete" {
			return errFailed
		}
		return nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	lruStore, err := New(ctx, dstore, testCapacity)
	if err != nil {
		t.Fatal(err)
	}

	// Test LRUStore eviction.
	for i := 0; i < lruStore.Cap(); i++ {
		k := ds.NewKey(fmt.Sprintf("key-%d", i))
		err = lruStore.Put(ctx, k, []byte(fmt.Sprintf("val-%d", i)))
		if err != nil {
			t.Fatal(err)
		}
	}

	key := ds.NewKey("hw")
	val := []byte("hello world")

	// Test Put fails when eviction fails.
	err = lruStore.Put(ctx, key, val)
	if err != errFailed {
		t.Fatalf("expected error %q", errFailed)
	}

	// Check that failed put did not put key into LRUStore.
	_, err = lruStore.Get(ctx, key)
	if err != ds.ErrNotFound {
		t.Fatalf("Expected error %s", ds.ErrNotFound)
	}

	// Test Delete fails when eviction fails.
	err = lruStore.Delete(ctx, key)
	if err != errFailed {
		t.Fatalf("expected error %q", errFailed)
	}

	// Test Resize fails when eviction fails.
	diff, err := lruStore.Resize(ctx, lruStore.Len()-1)
	if err != errFailed {
		t.Fatalf("expected error %q", errFailed)
	}
	if diff != 0 {
		t.Fatal("expected diff to be 0 on error")
	}

	// Test Clear fails when eviction fails.
	err = lruStore.Clear(ctx)
	if err != errFailed {
		t.Fatalf("expected error %q", errFailed)
	}
}

func TestGetFail(t *testing.T) {
	errFailed := errors.New("failed")
	dstore := fs.NewFailstore(ds.NewMapDatastore(), func(op string) error {
		if op == "get" {
			return errFailed
		}
		return nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	lruStore, err := New(ctx, dstore, testCapacity)
	if err != nil {
		t.Fatal(err)
	}

	key := ds.NewKey("hw")
	val := []byte("hello world")

	// Test Put and Get.
	err = lruStore.Put(ctx, key, val)
	if err != nil {
		t.Fatal(err)
	}

	_, err = lruStore.Get(ctx, key)
	if err != errFailed {
		t.Fatalf("expected error %q", errFailed)
	}

}

func TestPutFail(t *testing.T) {
	errFailed := errors.New("failed")
	dstore := fs.NewFailstore(ds.NewMapDatastore(), func(op string) error {
		if op == "put" {
			return errFailed
		}
		return nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	lruStore, err := New(ctx, dstore, testCapacity)
	if err != nil {
		t.Fatal(err)
	}

	key := ds.NewKey("hw")
	val := []byte("hello world")

	// Test Put and Get.
	err = lruStore.Put(ctx, key, val)
	if err != errFailed {
		t.Fatalf("expected error %q", errFailed)
	}
}

func TestQueryFail(t *testing.T) {
	errFailed := errors.New("failed")
	enableFail := true
	dstore := fs.NewFailstore(ds.NewMapDatastore(), func(op string) error {
		if enableFail && op == "query" {
			return errFailed
		}
		return nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := New(ctx, dstore, testCapacity)
	if err != errFailed {
		t.Fatalf("expected error %q", errFailed)
	}

	enableFail = false

	lruStore, err := New(ctx, dstore, testCapacity)
	if err != nil {
		t.Fatal(err)
	}

	enableFail = true

	key := ds.NewKey("hw")
	val := []byte("hello world")

	// Test Put and Get.
	err = lruStore.Put(ctx, key, val)
	if err != nil {
		t.Fatal(err)
	}

	q := dsq.Query{
		KeysOnly: true,
	}
	_, err = lruStore.Query(ctx, q)
	if err != errFailed {
		t.Fatalf("expected error %q", errFailed)
	}

	// Test that New fails when context is canceled.
	enableFail = false
	cancel()
	_, err = New(ctx, dstore, testCapacity)
	if err != context.Canceled {
		t.Fatalf("expected error %q", context.Canceled)
	}

}
