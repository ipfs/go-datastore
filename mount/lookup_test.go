package mount

import (
	"testing"

	datastore "github.com/ipfs/go-datastore"
)

func TestLookup(t *testing.T) {
	mapds0 := datastore.NewMapDatastore()
	mapds1 := datastore.NewMapDatastore()
	mapds2 := datastore.NewMapDatastore()
	mapds3 := datastore.NewMapDatastore()
	m := New([]Mount{
		{Prefix: datastore.NewKey("/"), Datastore: mapds0},
		{Prefix: datastore.NewKey("/foo"), Datastore: mapds1},
		{Prefix: datastore.NewKey("/bar"), Datastore: mapds2},
		{Prefix: datastore.NewKey("/baz"), Datastore: mapds3},
	})
	_, mnts, _ := m.lookupAll(datastore.NewKey("/bar"))
	if len(mnts) != 1 || mnts[0] != datastore.NewKey("/bar") {
		t.Errorf("expected to find the mountpoint /bar, got %v", mnts)
	}

	_, mnts, _ = m.lookupAll(datastore.NewKey("/fo"))
	if len(mnts) != 1 || mnts[0] != datastore.NewKey("/") {
		t.Errorf("expected to find the mountpoint /, got %v", mnts)
	}

	_, mnt, _ := m.lookup(datastore.NewKey("/fo"))
	if mnt != datastore.NewKey("/") {
		t.Errorf("expected to find the mountpoint /, got %v", mnt)
	}

	// /foo lives in /, /foo/bar lives in /foo. Most systems don't let us use the key "" or /.
	_, mnt, _ = m.lookup(datastore.NewKey("/foo"))
	if mnt != datastore.NewKey("/") {
		t.Errorf("expected to find the mountpoint /, got %v", mnt)
	}

	_, mnt, _ = m.lookup(datastore.NewKey("/foo/bar"))
	if mnt != datastore.NewKey("/foo") {
		t.Errorf("expected to find the mountpoint /foo, got %v", mnt)
	}
}
