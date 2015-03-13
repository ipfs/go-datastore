package mount_test

import (
	"testing"

	"github.com/jbenet/go-datastore"
	"github.com/jbenet/go-datastore/mount"
)

func TestPutBadNothing(t *testing.T) {
	m := mount.New(nil)

	err := m.Put(datastore.NewKey("quux"), []byte("foobar"))
	if g, e := err, mount.ErrNoMount; g != e {
		t.Fatalf("Put got wrong error: %v != %v", g, e)
	}
}

func TestPutBadNoMount(t *testing.T) {
	mapds := datastore.NewMapDatastore()
	m := mount.New([]mount.Mount{
		{Prefix: datastore.NewKey("/redherring"), Datastore: mapds},
	})

	err := m.Put(datastore.NewKey("/quux/thud"), []byte("foobar"))
	if g, e := err, mount.ErrNoMount; g != e {
		t.Fatalf("expected ErrNoMount, got: %v\n", g)
	}
}

func TestPut(t *testing.T) {
	mapds := datastore.NewMapDatastore()
	m := mount.New([]mount.Mount{
		{Prefix: datastore.NewKey("/quux"), Datastore: mapds},
	})

	if err := m.Put(datastore.NewKey("/quux/thud"), []byte("foobar")); err != nil {
		t.Fatalf("Put error: %v", err)
	}

	val, err := mapds.Get(datastore.NewKey("/thud"))
	if err != nil {
		t.Fatalf("Get error: %v", err)
	}
	buf, ok := val.([]byte)
	if !ok {
		t.Fatalf("Get value is not []byte: %T %v", val, val)
	}
	if g, e := string(buf), "foobar"; g != e {
		t.Errorf("wrong value: %q != %q", g, e)
	}
}

func TestGetBadNothing(t *testing.T) {
	m := mount.New([]mount.Mount{})

	_, err := m.Get(datastore.NewKey("/quux/thud"))
	if g, e := err, datastore.ErrNotFound; g != e {
		t.Fatalf("expected ErrNotFound, got: %v\n", g)
	}
}

func TestGetBadNoMount(t *testing.T) {
	mapds := datastore.NewMapDatastore()
	m := mount.New([]mount.Mount{
		{Prefix: datastore.NewKey("/redherring"), Datastore: mapds},
	})

	_, err := m.Get(datastore.NewKey("/quux/thud"))
	if g, e := err, datastore.ErrNotFound; g != e {
		t.Fatalf("expected ErrNotFound, got: %v\n", g)
	}
}

func TestGetNotFound(t *testing.T) {
	mapds := datastore.NewMapDatastore()
	m := mount.New([]mount.Mount{
		{Prefix: datastore.NewKey("/quux"), Datastore: mapds},
	})

	_, err := m.Get(datastore.NewKey("/quux/thud"))
	if g, e := err, datastore.ErrNotFound; g != e {
		t.Fatalf("expected ErrNotFound, got: %v\n", g)
	}
}

func TestGet(t *testing.T) {
	mapds := datastore.NewMapDatastore()
	m := mount.New([]mount.Mount{
		{Prefix: datastore.NewKey("/quux"), Datastore: mapds},
	})

	if err := mapds.Put(datastore.NewKey("/thud"), []byte("foobar")); err != nil {
		t.Fatalf("Get error: %v", err)
	}

	val, err := m.Get(datastore.NewKey("/quux/thud"))
	if err != nil {
		t.Fatalf("Put error: %v", err)
	}

	buf, ok := val.([]byte)
	if !ok {
		t.Fatalf("Get value is not []byte: %T %v", val, val)
	}
	if g, e := string(buf), "foobar"; g != e {
		t.Errorf("wrong value: %q != %q", g, e)
	}
}
