package keytransform_test

import (
	"context"
	"slices"
	"testing"

	ds "github.com/ipfs/go-datastore"
	kt "github.com/ipfs/go-datastore/keytransform"
	dsq "github.com/ipfs/go-datastore/query"
	dstest "github.com/ipfs/go-datastore/test"
	"github.com/stretchr/testify/require"
)

var pair = &kt.Pair{
	Convert: func(k ds.Key) ds.Key {
		return ds.NewKey("/abc").Child(k)
	},
	Invert: func(k ds.Key) ds.Key {
		// remove abc prefix
		l := k.List()
		if l[0] != "abc" {
			panic("key does not have prefix. convert failed?")
		}
		return ds.KeyWithNamespaces(l[1:])
	},
}

func TestBasic(t *testing.T) {
	ctx := context.Background()

	mpds := dstest.NewTestDatastore(true)
	ktds := kt.Wrap(mpds, pair)

	keys := strsToKeys([]string{
		"foo",
		"foo/bar",
		"foo/bar/baz",
		"foo/barb",
		"foo/bar/bazb",
		"foo/bar/baz/barb",
	})

	for _, k := range keys {
		err := ktds.Put(ctx, k, []byte(k.String()))
		require.NoError(t, err)
	}

	for _, k := range keys {
		v1, err := ktds.Get(ctx, k)
		require.NoError(t, err)
		require.Equal(t, []byte(k.String()), v1)

		v2, err := mpds.Get(ctx, ds.NewKey("abc").Child(k))
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
	listB := run(ktds, dsq.Query{})
	require.Equal(t, len(listA), len(listB))

	// sort them cause yeah.
	slices.SortFunc(listA, func(a, b ds.Key) int {
		return a.Compare(b)
	})
	slices.SortFunc(listB, func(a, b ds.Key) int {
		return a.Compare(b)
	})

	for i, kA := range listA {
		kB := listB[i]
		require.Equal(t, kB, pair.Invert(kA))
		require.Equal(t, pair.Convert(kB), kA)
	}

	t.Log("listA: ", listA)
	t.Log("listB: ", listB)

	require.ErrorIs(t, ktds.Check(ctx), dstest.ErrTest)
	require.ErrorIs(t, ktds.CollectGarbage(ctx), dstest.ErrTest)
	require.ErrorIs(t, ktds.Scrub(ctx), dstest.ErrTest)
}

func strsToKeys(strs []string) []ds.Key {
	keys := make([]ds.Key, len(strs))
	for i, s := range strs {
		keys[i] = ds.NewKey(s)
	}
	return keys
}

func TestSuiteDefaultPair(t *testing.T) {
	mpds := dstest.NewTestDatastore(true)
	ktds := kt.Wrap(mpds, pair)
	dstest.SubtestAll(t, ktds)
}

func TestSuitePrefixTransform(t *testing.T) {
	mpds := dstest.NewTestDatastore(true)
	ktds := kt.Wrap(mpds, kt.PrefixTransform{Prefix: ds.NewKey("/foo")})
	dstest.SubtestAll(t, ktds)
}
