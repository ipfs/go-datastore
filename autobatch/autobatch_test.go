package autobatch

import (
	"bytes"
	"fmt"
	"testing"

	ds "github.com/ipfs/go-datastore"
	dstest "github.com/ipfs/go-datastore/test"
)

func TestAutobatch(t *testing.T) {
	dstest.SubtestAll(t, NewAutoBatching(ds.NewMapDatastore(), 16))
}

func TestFlushing(t *testing.T) {
	child := ds.NewMapDatastore()
	d := NewAutoBatching(child, 16)

	var keys []ds.Key
	for i := 0; i < 16; i++ {
		keys = append(keys, ds.NewKey(fmt.Sprintf("test%d", i)))
	}
	v := []byte("hello world")

	for _, k := range keys {
		err := d.Put(k, v)
		if err != nil {
			t.Fatal(err)
		}
	}

	// Get works normally.
	for _, k := range keys {
		val, err := d.Get(k)
		if err != nil {
			t.Fatal(err)
		}

		if !bytes.Equal(val, v) {
			t.Fatal("wrong value")
		}
	}

	// Not flushed
	_, err := child.Get(keys[0])
	if err != ds.ErrNotFound {
		t.Fatal("shouldnt have found value")
	}

	// Delete works.
	err = d.Delete(keys[14])
	if err != nil {
		t.Fatal(err)
	}
	_, err = d.Get(keys[14])
	if err != ds.ErrNotFound {
		t.Fatal(err)
	}

	// Still not flushed
	_, err = child.Get(keys[0])
	if err != ds.ErrNotFound {
		t.Fatal("shouldnt have found value")
	}

	// Final put flushes.
	err = d.Put(ds.NewKey("test16"), v)
	if err != nil {
		t.Fatal(err)
	}

	// should be flushed now, try to get keys from child datastore
	for _, k := range keys[:14] {
		val, err := child.Get(k)
		if err != nil {
			t.Fatal(err)
		}

		if !bytes.Equal(val, v) {
			t.Fatal("wrong value")
		}
	}

	// Never flushed the deleted key.
	_, err = child.Get(keys[14])
	if err != ds.ErrNotFound {
		t.Fatal("shouldnt have found value")
	}

	// Delete doesn't flush
	err = d.Delete(keys[0])
	if err != nil {
		t.Fatal(err)
	}

	val, err := child.Get(keys[0])
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(val, v) {
		t.Fatal("wrong value")
	}
}
