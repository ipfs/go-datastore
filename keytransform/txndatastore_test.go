package keytransform_test

import (
	"bytes"
	"context"
	"fmt"
	"sort"

	. "gopkg.in/check.v1"

	ds "github.com/ipfs/go-datastore"
	kt "github.com/ipfs/go-datastore/keytransform"
	dsq "github.com/ipfs/go-datastore/query"
	dstest "github.com/ipfs/go-datastore/test"
)

var _ = Suite(&DSSuite{})

func (ks *DSSuite) TestWrapTxnDatastoreBasic(c *C) {
	ctx := context.Background()
	ms := ds.NewMapDatastore()
	mpds := dstest.NewTestTxnDatastore(ms, true)

	kt.WrapTxnDatastore(mpds, pair)
	ktds := kt.WrapTxnDatastore(mpds, pair)
	ktdsTx, err := ktds.NewTransaction(ctx, false)
	c.Check(err, Equals, nil)

	keys := strsToKeys([]string{
		"foo",
		"foo/bar",
		"foo/bar/baz",
		"foo/barb",
		"foo/bar/bazb",
		"foo/bar/baz/barb",
	})

	for _, k := range keys {
		err := ktdsTx.Put(ctx, k, []byte(k.String()))
		c.Check(err, Equals, nil)
	}

	for _, k := range keys {
		// underlying mapstore can only see committed results
		_, err := ms.Get(ctx, k)
		c.Check(err, Equals, ds.ErrNotFound)

		_, err = ms.Get(ctx, ds.NewKey("abc").Child(k))
		c.Check(err, Equals, ds.ErrNotFound)

		v1, err := ktdsTx.Get(ctx, k)
		c.Check(err, Equals, nil)
		c.Check(bytes.Equal(v1, []byte(k.String())), Equals, true)

		// underlying TxnDatastore can only see committed results
		_, err = mpds.Get(ctx, ds.NewKey("abc").Child(k))
		c.Check(err, Equals, ds.ErrNotFound)
	}

	run := func(d ds.Read, q dsq.Query) []ds.Key {
		r, err := d.Query(ctx, q)
		c.Check(err, Equals, nil)

		e, err := r.Rest()
		c.Check(err, Equals, nil)

		return ds.EntryKeys(e)
	}

	listA := run(mpds, dsq.Query{})
	listB := run(ktdsTx, dsq.Query{})
	if len(listA) == len(listB) {
		c.Errorf("TxnDatastore and WrappedTxDatastore should not have equal Query results pre-commit")
	}

	if err := ktds.Check(ctx); err != dstest.ErrTest {
		c.Errorf("Unexpected Check() error: %s", err)
	}

	if err := ktds.CollectGarbage(ctx); err != dstest.ErrTest {
		c.Errorf("Unexpected CollectGarbage() error: %s", err)
	}

	if err := ktds.Scrub(ctx); err != dstest.ErrTest {
		c.Errorf("Unexpected Scrub() error: %s", err)
	}

	// Commit wrapped tx and compare
	err = ktdsTx.Commit(ctx)
	c.Check(err, Equals, nil)

	for _, k := range keys {
		// results should be committed to the underlying mapstore
		_, err = ms.Get(ctx, k)
		c.Check(err, Equals, ds.ErrNotFound)

		v0, err := ms.Get(ctx, ds.NewKey("abc").Child(k))
		c.Check(err, Equals, nil)
		c.Check(bytes.Equal(v0, []byte(k.String())), Equals, true)

		v1, err := ktdsTx.Get(ctx, k)
		c.Check(err, Equals, nil)
		c.Check(bytes.Equal(v1, []byte(k.String())), Equals, true)

		// results should be committed to the wrapped TxnDatastore
		v2, err := mpds.Get(ctx, ds.NewKey("abc").Child(k))
		c.Check(err, Equals, nil)
		c.Check(bytes.Equal(v2, []byte(k.String())), Equals, true)
	}

	listA = run(mpds, dsq.Query{})
	listB = run(ktdsTx, dsq.Query{})
	listC := run(ms, dsq.Query{})
	c.Check(len(listA), Equals, len(listB))
	c.Check(len(listA), Equals, len(listC))

	// sort them cause yeah.
	sort.Sort(ds.KeySlice(listA))
	sort.Sort(ds.KeySlice(listB))
	sort.Sort(ds.KeySlice(listC))

	for i, kA := range listA {
		kB := listB[i]
		kC := listC[i]
		c.Check(pair.Invert(kA), Equals, kB)
		c.Check(kA, Equals, pair.Convert(kB))
		c.Check(kC, Equals, kA)
	}

	c.Log("listA: ", listA)
	c.Log("listB: ", listB)
	c.Log("listC: ", listC)

	// Create a new tx and add some uncommitted values to
	ktdsTx, err = ktds.NewTransaction(ctx, false)
	c.Check(err, Equals, nil)

	unCommittedKeys := strsToKeys([]string{
		"foo",
		"foo/bar",
		"foo/bar/baz",
	})
	unCommittedKeysMap := make(map[ds.Key][]byte)
	for i, k := range unCommittedKeys {
		unCommittedKeysMap[k] = []byte(fmt.Sprintf("overwrite value %d", i))
	}
	for k, val := range unCommittedKeysMap {
		err := ktdsTx.Put(ctx, k, val)
		c.Check(err, Equals, nil)
	}

	for _, k := range keys {
		// underlying mapstore will have only the committed results
		_, err = ms.Get(ctx, k)
		c.Check(err, Equals, ds.ErrNotFound)

		v0, err := ms.Get(ctx, ds.NewKey("abc").Child(k))
		c.Check(err, Equals, nil)
		c.Check(bytes.Equal(v0, []byte(k.String())), Equals, true)

		// tx will return a mixture of the pending results and committed results
		v1, err := ktdsTx.Get(ctx, k)
		c.Check(err, Equals, nil)
		if val, ok := unCommittedKeysMap[k]; ok {
			c.Check(bytes.Equal(v1, val), Equals, true)
		} else {
			c.Check(bytes.Equal(v1, []byte(k.String())), Equals, true)
		}

		// underlying TxnDatastore will have only the committed results
		v2, err := mpds.Get(ctx, ds.NewKey("abc").Child(k))
		c.Check(err, Equals, nil)
		c.Check(bytes.Equal(v2, []byte(k.String())), Equals, true)
	}
}
