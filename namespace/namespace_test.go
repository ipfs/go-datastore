package namespace_test

import (
	"context"
	"sort"
	"testing"

	ds "github.com/ipfs/go-datastore"
	ns "github.com/ipfs/go-datastore/namespace"
	dsq "github.com/ipfs/go-datastore/query"
	dstest "github.com/ipfs/go-datastore/test"
	"github.com/stretchr/testify/require"
)

func TestBasic(t *testing.T) {
	testBasic(t, "abc")
	testBasic(t, "")
}

func testBasic(t *testing.T, prefix string) {
	ctx := context.Background()

	mpds := ds.NewMapDatastore()
	nsds := ns.Wrap(mpds, ds.NewKey(prefix))

	keys := strsToKeys([]string{
		"foo",
		"foo/bar",
		"foo/bar/baz",
		"foo/barb",
		"foo/bar/bazb",
		"foo/bar/baz/barb",
	})

	for _, k := range keys {
		err := nsds.Put(ctx, k, []byte(k.String()))
		require.NoError(t, err)
	}

	for _, k := range keys {
		v1, err := nsds.Get(ctx, k)
		require.NoError(t, err)
		require.Equal(t, []byte(k.String()), v1)

		v2, err := mpds.Get(ctx, ds.NewKey(prefix).Child(k))
		require.NoError(t, err)
		require.Equal(t, []byte(k.String()), v2)
	}

	run := func(d ds.Datastore, q dsq.Query) []ds.Key {
		r, err := d.Query(ctx, q)
		require.NoError(t, err)

		e, err := r.Rest()
		require.NoError(t, err)

		return ds.EntryKeys(e)
	}

	listA := run(mpds, dsq.Query{})
	listB := run(nsds, dsq.Query{})
	require.Equal(t, len(listA), len(listB))

	// sort them cause yeah.
	sort.Sort(ds.KeySlice(listA))
	sort.Sort(ds.KeySlice(listB))

	for i, kA := range listA {
		kB := listB[i]
		require.Equal(t, kB, nsds.InvertKey(kA))
		require.Equal(t, nsds.ConvertKey(kB), kA)
	}
}

func TestQuery(t *testing.T) {
	ctx := context.Background()

	mpds := dstest.NewTestDatastore(true)
	nsds := ns.Wrap(mpds, ds.NewKey("/foo"))

	keys := strsToKeys([]string{
		"abc/foo",
		"bar/foo",
		"foo/bar",
		"foo/bar/baz",
		"foo/baz/abc",
		"xyz/foo",
	})

	for _, k := range keys {
		err := mpds.Put(ctx, k, []byte(k.String()))
		require.NoError(t, err)
	}

	qres, err := nsds.Query(ctx, dsq.Query{})
	require.NoError(t, err)

	expect := []dsq.Entry{
		{Key: "/bar", Size: len([]byte("/foo/bar")), Value: []byte("/foo/bar")},
		{Key: "/bar/baz", Size: len([]byte("/foo/bar/baz")), Value: []byte("/foo/bar/baz")},
		{Key: "/baz/abc", Size: len([]byte("/foo/baz/abc")), Value: []byte("/foo/baz/abc")},
	}

	results, err := qres.Rest()
	require.NoError(t, err)
	sort.Slice(results, func(i, j int) bool { return results[i].Key < results[j].Key })

	for i, ent := range results {
		require.Equal(t, expect[i].Key, ent.Key)
		require.Equal(t, string(expect[i].Value), string(ent.Value))
	}

	qres.Close()

	qres, err = nsds.Query(ctx, dsq.Query{Prefix: "bar"})
	require.NoError(t, err)

	expect = []dsq.Entry{
		{Key: "/bar/baz", Size: len([]byte("/foo/bar/baz")), Value: []byte("/foo/bar/baz")},
	}

	results, err = qres.Rest()
	require.NoError(t, err)
	sort.Slice(results, func(i, j int) bool { return results[i].Key < results[j].Key })

	for i, ent := range results {
		require.Equal(t, expect[i].Key, ent.Key)
		require.Equal(t, string(expect[i].Value), string(ent.Value))
	}

	require.ErrorIs(t, nsds.Check(ctx), dstest.ErrTest)
	require.ErrorIs(t, nsds.CollectGarbage(ctx), dstest.ErrTest)
	require.ErrorIs(t, nsds.Scrub(ctx), dstest.ErrTest)
}

func strsToKeys(strs []string) []ds.Key {
	keys := make([]ds.Key, len(strs))
	for i, s := range strs {
		keys[i] = ds.NewKey(s)
	}
	return keys
}

func TestSuite(t *testing.T) {
	mpds := dstest.NewTestDatastore(true)
	nsds := ns.Wrap(mpds, ds.NewKey("/foo"))
	dstest.SubtestAll(t, nsds)
}
