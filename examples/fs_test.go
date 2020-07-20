package examples

import (
	"bytes"
	"context"
	"testing"

	. "gopkg.in/check.v1"

	ds "github.com/ipfs/go-datastore"
	query "github.com/ipfs/go-datastore/query"
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) { TestingT(t) }

type DSSuite struct {
	dir string
	ds  ds.Datastore
}

var _ = Suite(&DSSuite{})

func (ks *DSSuite) SetUpTest(c *C) {
	ks.dir = c.MkDir()
	ks.ds, _ = NewDatastore(ks.dir)
}

func (ks *DSSuite) TestOpen(c *C) {
	_, err := NewDatastore("/tmp/foo/bar/baz")
	c.Assert(err, Not(Equals), nil)

	// setup ds
	_, err = NewDatastore(ks.dir)
	c.Assert(err, Equals, nil)
}

func (ks *DSSuite) TestBasic(c *C) {
	ctx := context.Background()

	keys := strsToKeys([]string{
		"foo",
		"foo/bar",
		"foo/bar/baz",
		"foo/barb",
		"foo/bar/bazb",
		"foo/bar/baz/barb",
	})

	for _, k := range keys {
		err := ks.ds.Put(ctx, k, []byte(k.String()))
		c.Check(err, Equals, nil)
	}

	for _, k := range keys {
		v, err := ks.ds.Get(ctx, k)
		c.Check(err, Equals, nil)
		c.Check(bytes.Equal(v, []byte(k.String())), Equals, true)
	}

	r, err := ks.ds.Query(ctx, query.Query{Prefix: "/foo/bar/"})
	if err != nil {
		c.Check(err, Equals, nil)
	}

	expect := []string{
		"/foo/bar/baz",
		"/foo/bar/bazb",
		"/foo/bar/baz/barb",
	}
	all, err := r.Rest()
	if err != nil {
		c.Fatal(err)
	}
	c.Check(len(all), Equals, len(expect))

	for _, k := range expect {
		found := false
		for _, e := range all {
			if e.Key == k {
				found = true
			}
		}

		if !found {
			c.Error("did not find expected key: ", k)
		}
	}
}

func (ks *DSSuite) TestDiskUsage(c *C) {
	ctx := context.Background()

	keys := strsToKeys([]string{
		"foo",
		"foo/bar",
		"foo/bar/baz",
		"foo/barb",
		"foo/bar/bazb",
		"foo/bar/baz/barb",
	})

	totalBytes := 0
	for _, k := range keys {
		value := []byte(k.String())
		totalBytes += len(value)
		err := ks.ds.Put(ctx, k, value)
		c.Check(err, Equals, nil)
	}

	if ps, ok := ks.ds.(ds.PersistentDatastore); ok {
		if s, err := ps.DiskUsage(); s != uint64(totalBytes) || err != nil {
			c.Error("unexpected size is: ", s)
		}
	} else {
		c.Error("should implement PersistentDatastore")
	}
}

func strsToKeys(strs []string) []ds.Key {
	keys := make([]ds.Key, len(strs))
	for i, s := range strs {
		keys[i] = ds.NewKey(s)
	}
	return keys
}
