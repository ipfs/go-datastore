package examples

import (
	"context"
	"testing"

	ds "github.com/ipfs/go-datastore"
	query "github.com/ipfs/go-datastore/query"
	"github.com/stretchr/testify/require"
)

func TestOpen(t *testing.T) {
	_, err := NewDatastore("/tmp/foo/bar/baz")
	require.Error(t, err)

	// setup ds
	_, err = NewDatastore(t.TempDir())
	require.NoError(t, err)
}

func TestBasic(t *testing.T) {
	ctx := context.Background()

	keys := strsToKeys([]string{
		"foo",
		"foo/bar",
		"foo/bar/baz",
		"foo/barb",
		"foo/bar/bazb",
		"foo/bar/baz/barb",
	})

	dstore, err := NewDatastore(t.TempDir())
	require.NoError(t, err)
	for _, k := range keys {
		err := dstore.Put(ctx, k, []byte(k.String()))
		require.NoError(t, err)
	}

	for _, k := range keys {
		v, err := dstore.Get(ctx, k)
		require.NoError(t, err)
		require.Equal(t, []byte(k.String()), v)
	}

	r, err := dstore.Query(ctx, query.Query{Prefix: "/foo/bar/"})
	require.NoError(t, err)

	expect := []string{
		"/foo/bar/baz",
		"/foo/bar/bazb",
		"/foo/bar/baz/barb",
	}
	all, err := r.Rest()
	require.NoError(t, err)
	require.Equal(t, len(all), len(expect))

	for _, k := range expect {
		found := false
		for _, e := range all {
			if e.Key == k {
				found = true
				break
			}
		}

		require.True(t, found, "did not find expected key:", k)
	}
}

func TestDiskUsage(t *testing.T) {
	ctx := context.Background()

	keys := strsToKeys([]string{
		"foo",
		"foo/bar",
		"foo/bar/baz",
		"foo/barb",
		"foo/bar/bazb",
		"foo/bar/baz/barb",
	})

	dstore, err := NewDatastore(t.TempDir())
	require.NoError(t, err)

	totalBytes := 0
	for _, k := range keys {
		value := []byte(k.String())
		totalBytes += len(value)
		err := dstore.Put(ctx, k, value)
		require.NoError(t, err)
	}

	ps, ok := dstore.(ds.PersistentDatastore)
	require.True(t, ok, "should implement PersistentDatastore")

	s, err := ps.DiskUsage(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(totalBytes), s, "unexpected size")
}

func strsToKeys(strs []string) []ds.Key {
	keys := make([]ds.Key, len(strs))
	for i, s := range strs {
		keys[i] = ds.NewKey(s)
	}
	return keys
}
