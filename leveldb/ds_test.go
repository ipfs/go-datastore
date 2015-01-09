package leveldb

import (
	"io/ioutil"
	"os"
	"testing"

	ds "github.com/jbenet/go-datastore"
	dsq "github.com/jbenet/go-datastore/query"
)

var testcases = map[string]string{
	"/a":     "a",
	"/a/b":   "ab",
	"/a/b/c": "abc",
	"/a/b/d": "a/b/d",
	"/a/c":   "ac",
	"/a/d":   "ad",
	"/e":     "e",
	"/f":     "f",
}

func TestQuery(t *testing.T) {
	path, err := ioutil.TempDir("/tmp", "testing_leveldb_")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		os.RemoveAll(path)
	}()

	d, err := NewDatastore(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	for k, v := range testcases {
		dsk := ds.NewKey(k)
		if err := d.Put(dsk, []byte(v)); err != nil {
			t.Fatal(err)
		}
	}

	for k, v := range testcases {
		dsk := ds.NewKey(k)
		v2, err := d.Get(dsk)
		if err != nil {
			t.Fatal(err)
		}
		v2b := v2.([]byte)
		if string(v2b) != v {
			t.Errorf("%s values differ: %s != %s", k, v, v2)
		}
	}

	rs, err := d.Query(dsq.Query{Prefix: "/a/"})
	if err != nil {
		t.Fatal(err)
	}

	expect := []string{
		"/a/b",
		"/a/b/c",
		"/a/b/d",
		"/a/c",
		"/a/d",
	}

	re := rs.AllEntries()
	if len(re) != len(expect) {
		t.Error("not enough", expect, re)
	}
	for _, k := range expect {
		found := false
		for _, e := range re {
			if e.Key == k {
				found = true
			}
		}
		if !found {
			t.Error(k, "not found")
		}
	}
}
