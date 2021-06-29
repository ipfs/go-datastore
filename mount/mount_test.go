package mount_test

import (
	"context"
	"errors"
	"testing"

	datastore "github.com/ipfs/go-datastore"
	autobatch "github.com/ipfs/go-datastore/autobatch"
	mount "github.com/ipfs/go-datastore/mount"
	query "github.com/ipfs/go-datastore/query"
	sync "github.com/ipfs/go-datastore/sync"
	dstest "github.com/ipfs/go-datastore/test"
)

func TestPutBadNothing(t *testing.T) {
	ctx := context.Background()

	m := mount.New(nil)

	err := m.Put(ctx, datastore.NewKey("quux"), []byte("foobar"))
	if g, e := err, mount.ErrNoMount; g != e {
		t.Fatalf("Put got wrong error: %v != %v", g, e)
	}
}

func TestPutBadNoMount(t *testing.T) {
	ctx := context.Background()

	mapds := datastore.NewMapDatastore()
	m := mount.New([]mount.Mount{
		{Prefix: datastore.NewKey("/redherring"), Datastore: mapds},
	})

	err := m.Put(ctx, datastore.NewKey("/quux/thud"), []byte("foobar"))
	if g, e := err, mount.ErrNoMount; g != e {
		t.Fatalf("expected ErrNoMount, got: %v\n", g)
	}
}

func TestPut(t *testing.T) {
	ctx := context.Background()

	mapds := datastore.NewMapDatastore()
	m := mount.New([]mount.Mount{
		{Prefix: datastore.NewKey("/quux"), Datastore: mapds},
	})

	if err := m.Put(ctx, datastore.NewKey("/quux/thud"), []byte("foobar")); err != nil {
		t.Fatalf("Put error: %v", err)
	}

	buf, err := mapds.Get(ctx, datastore.NewKey("/thud"))
	if err != nil {
		t.Fatalf("Get error: %v", err)
	}
	if g, e := string(buf), "foobar"; g != e {
		t.Errorf("wrong value: %q != %q", g, e)
	}
}

func TestGetBadNothing(t *testing.T) {
	ctx := context.Background()

	m := mount.New([]mount.Mount{})

	_, err := m.Get(ctx, datastore.NewKey("/quux/thud"))
	if g, e := err, datastore.ErrNotFound; g != e {
		t.Fatalf("expected ErrNotFound, got: %v\n", g)
	}
}

func TestGetBadNoMount(t *testing.T) {
	ctx := context.Background()

	mapds := datastore.NewMapDatastore()
	m := mount.New([]mount.Mount{
		{Prefix: datastore.NewKey("/redherring"), Datastore: mapds},
	})

	_, err := m.Get(ctx, datastore.NewKey("/quux/thud"))
	if g, e := err, datastore.ErrNotFound; g != e {
		t.Fatalf("expected ErrNotFound, got: %v\n", g)
	}
}

func TestGetNotFound(t *testing.T) {
	ctx := context.Background()

	mapds := datastore.NewMapDatastore()
	m := mount.New([]mount.Mount{
		{Prefix: datastore.NewKey("/quux"), Datastore: mapds},
	})

	_, err := m.Get(ctx, datastore.NewKey("/quux/thud"))
	if g, e := err, datastore.ErrNotFound; g != e {
		t.Fatalf("expected ErrNotFound, got: %v\n", g)
	}
}

func TestGet(t *testing.T) {
	ctx := context.Background()

	mapds := datastore.NewMapDatastore()
	m := mount.New([]mount.Mount{
		{Prefix: datastore.NewKey("/quux"), Datastore: mapds},
	})

	if err := mapds.Put(ctx, datastore.NewKey("/thud"), []byte("foobar")); err != nil {
		t.Fatalf("Get error: %v", err)
	}

	buf, err := m.Get(ctx, datastore.NewKey("/quux/thud"))
	if err != nil {
		t.Fatalf("Put error: %v", err)
	}
	if g, e := string(buf), "foobar"; g != e {
		t.Errorf("wrong value: %q != %q", g, e)
	}
}

func TestHasBadNothing(t *testing.T) {
	ctx := context.Background()

	m := mount.New([]mount.Mount{})

	found, err := m.Has(ctx, datastore.NewKey("/quux/thud"))
	if err != nil {
		t.Fatalf("Has error: %v", err)
	}
	if g, e := found, false; g != e {
		t.Fatalf("wrong value: %v != %v", g, e)
	}
}

func TestHasBadNoMount(t *testing.T) {
	ctx := context.Background()

	mapds := datastore.NewMapDatastore()
	m := mount.New([]mount.Mount{
		{Prefix: datastore.NewKey("/redherring"), Datastore: mapds},
	})

	found, err := m.Has(ctx, datastore.NewKey("/quux/thud"))
	if err != nil {
		t.Fatalf("Has error: %v", err)
	}
	if g, e := found, false; g != e {
		t.Fatalf("wrong value: %v != %v", g, e)
	}
}

func TestHasNotFound(t *testing.T) {
	ctx := context.Background()

	mapds := datastore.NewMapDatastore()
	m := mount.New([]mount.Mount{
		{Prefix: datastore.NewKey("/quux"), Datastore: mapds},
	})

	found, err := m.Has(ctx, datastore.NewKey("/quux/thud"))
	if err != nil {
		t.Fatalf("Has error: %v", err)
	}
	if g, e := found, false; g != e {
		t.Fatalf("wrong value: %v != %v", g, e)
	}
}

func TestHas(t *testing.T) {
	ctx := context.Background()

	mapds := datastore.NewMapDatastore()
	m := mount.New([]mount.Mount{
		{Prefix: datastore.NewKey("/quux"), Datastore: mapds},
	})

	if err := mapds.Put(ctx, datastore.NewKey("/thud"), []byte("foobar")); err != nil {
		t.Fatalf("Put error: %v", err)
	}

	found, err := m.Has(ctx, datastore.NewKey("/quux/thud"))
	if err != nil {
		t.Fatalf("Has error: %v", err)
	}
	if g, e := found, true; g != e {
		t.Fatalf("wrong value: %v != %v", g, e)
	}
}

func TestDeleteNotFound(t *testing.T) {
	ctx := context.Background()

	mapds := datastore.NewMapDatastore()
	m := mount.New([]mount.Mount{
		{Prefix: datastore.NewKey("/quux"), Datastore: mapds},
	})

	err := m.Delete(ctx, datastore.NewKey("/quux/thud"))
	if err != nil {
		t.Fatalf("expected nil, got: %v\n", err)
	}
}

func TestDelete(t *testing.T) {
	ctx := context.Background()

	mapds := datastore.NewMapDatastore()
	m := mount.New([]mount.Mount{
		{Prefix: datastore.NewKey("/quux"), Datastore: mapds},
	})

	if err := mapds.Put(ctx, datastore.NewKey("/thud"), []byte("foobar")); err != nil {
		t.Fatalf("Put error: %v", err)
	}

	err := m.Delete(ctx, datastore.NewKey("/quux/thud"))
	if err != nil {
		t.Fatalf("Delete error: %v", err)
	}

	// make sure it disappeared
	found, err := mapds.Has(ctx, datastore.NewKey("/thud"))
	if err != nil {
		t.Fatalf("Has error: %v", err)
	}
	if g, e := found, false; g != e {
		t.Fatalf("wrong value: %v != %v", g, e)
	}
}

func TestQuerySimple(t *testing.T) {
	ctx := context.Background()

	mapds := datastore.NewMapDatastore()
	m := mount.New([]mount.Mount{
		{Prefix: datastore.NewKey("/quux"), Datastore: mapds},
	})

	const myKey = "/quux/thud"
	if err := m.Put(ctx, datastore.NewKey(myKey), []byte("foobar")); err != nil {
		t.Fatalf("Put error: %v", err)
	}

	res, err := m.Query(ctx, query.Query{Prefix: "/quux"})
	if err != nil {
		t.Fatalf("Query fail: %v\n", err)
	}
	entries, err := res.Rest()
	if err != nil {
		t.Fatalf("Query Results.Rest fail: %v\n", err)
	}
	seen := false
	for _, e := range entries {
		switch e.Key {
		case datastore.NewKey(myKey).String():
			seen = true
		default:
			t.Errorf("saw unexpected key: %q", e.Key)
		}
	}
	if !seen {
		t.Errorf("did not see wanted key %q in %+v", myKey, entries)
	}

	err = res.Close()
	if err != nil {
		t.Errorf("result.Close failed %d", err)
	}
}

func TestQueryAcrossMounts(t *testing.T) {
	ctx := context.Background()

	mapds0 := datastore.NewMapDatastore()
	mapds1 := datastore.NewMapDatastore()
	mapds2 := datastore.NewMapDatastore()
	mapds3 := datastore.NewMapDatastore()
	m := mount.New([]mount.Mount{
		{Prefix: datastore.NewKey("/foo"), Datastore: mapds1},
		{Prefix: datastore.NewKey("/bar"), Datastore: mapds2},
		{Prefix: datastore.NewKey("/baz"), Datastore: mapds3},
		{Prefix: datastore.NewKey("/"), Datastore: mapds0},
	})

	if err := m.Put(ctx, datastore.NewKey("/foo/lorem"), []byte("123")); err != nil {
		t.Fatal(err)
	}
	if err := m.Put(ctx, datastore.NewKey("/bar/ipsum"), []byte("234")); err != nil {
		t.Fatal(err)
	}
	if err := m.Put(ctx, datastore.NewKey("/bar/dolor"), []byte("345")); err != nil {
		t.Fatal(err)
	}
	if err := m.Put(ctx, datastore.NewKey("/baz/sit"), []byte("456")); err != nil {
		t.Fatal(err)
	}
	if err := m.Put(ctx, datastore.NewKey("/banana"), []byte("567")); err != nil {
		t.Fatal(err)
	}

	expect := func(prefix string, values map[string]string) {
		t.Helper()
		res, err := m.Query(ctx, query.Query{Prefix: prefix})
		if err != nil {
			t.Fatalf("Query fail: %v\n", err)
		}
		entries, err := res.Rest()
		if err != nil {
			err = res.Close()
			if err != nil {
				t.Errorf("result.Close failed %d", err)
			}
			t.Fatalf("Query Results.Rest fail: %v\n", err)
		}
		if len(entries) != len(values) {
			t.Errorf("expected %d results, got %d", len(values), len(entries))
		}
		for _, e := range entries {
			v, ok := values[e.Key]
			if !ok {
				t.Errorf("unexpected key %s", e.Key)
				continue
			}

			if v != string(e.Value) {
				t.Errorf("key value didn't match expected %s: '%s' - '%s'", e.Key, v, e.Value)
			}

			values[e.Key] = "seen"
		}
	}

	expect("/ba", nil)
	expect("/bar", map[string]string{
		"/bar/ipsum": "234",
		"/bar/dolor": "345",
	})
	expect("/baz/", map[string]string{
		"/baz/sit": "456",
	})
	expect("/foo", map[string]string{
		"/foo/lorem": "123",
	})
	expect("/", map[string]string{
		"/foo/lorem": "123",
		"/bar/ipsum": "234",
		"/bar/dolor": "345",
		"/baz/sit":   "456",
		"/banana":    "567",
	})
	expect("/banana", nil)
}

func TestQueryAcrossMountsWithSort(t *testing.T) {
	ctx := context.Background()

	mapds0 := datastore.NewMapDatastore()
	mapds1 := datastore.NewMapDatastore()
	mapds2 := datastore.NewMapDatastore()
	m := mount.New([]mount.Mount{
		{Prefix: datastore.NewKey("/zoo"), Datastore: mapds1},
		{Prefix: datastore.NewKey("/boo/5"), Datastore: mapds2},
		{Prefix: datastore.NewKey("/boo"), Datastore: mapds0},
	})

	if err := m.Put(ctx, datastore.NewKey("/zoo/0"), []byte("123")); err != nil {
		t.Fatal(err)
	}
	if err := m.Put(ctx, datastore.NewKey("/zoo/1"), []byte("234")); err != nil {
		t.Fatal(err)
	}
	if err := m.Put(ctx, datastore.NewKey("/boo/9"), []byte("345")); err != nil {
		t.Fatal(err)
	}
	if err := m.Put(ctx, datastore.NewKey("/boo/3"), []byte("456")); err != nil {
		t.Fatal(err)
	}
	if err := m.Put(ctx, datastore.NewKey("/boo/5/hello"), []byte("789")); err != nil {
		t.Fatal(err)
	}

	res, err := m.Query(ctx, query.Query{Orders: []query.Order{query.OrderByKey{}}})
	if err != nil {
		t.Fatalf("Query fail: %v\n", err)
	}
	entries, err := res.Rest()
	if err != nil {
		t.Fatalf("Query Results.Rest fail: %v\n", err)
	}

	expect := []string{
		"/boo/3",
		"/boo/5/hello",
		"/boo/9",
		"/zoo/0",
		"/zoo/1",
	}

	if len(entries) != len(expect) {
		t.Fatalf("expected %d entries, but got %d", len(expect), len(entries))
	}

	for i, e := range expect {
		if e != entries[i].Key {
			t.Errorf("expected key %s, but got %s", e, entries[i].Key)
		}
	}

	err = res.Close()
	if err != nil {
		t.Errorf("result.Close failed %d", err)
	}
}

func TestQueryLimitAcrossMountsWithSort(t *testing.T) {
	ctx := context.Background()

	mapds1 := sync.MutexWrap(datastore.NewMapDatastore())
	mapds2 := sync.MutexWrap(datastore.NewMapDatastore())
	mapds3 := sync.MutexWrap(datastore.NewMapDatastore())
	m := mount.New([]mount.Mount{
		{Prefix: datastore.NewKey("/rok"), Datastore: mapds1},
		{Prefix: datastore.NewKey("/zoo"), Datastore: mapds2},
		{Prefix: datastore.NewKey("/noop"), Datastore: mapds3},
	})

	if err := m.Put(ctx, datastore.NewKey("/rok/0"), []byte("ghi")); err != nil {
		t.Fatal(err)
	}
	if err := m.Put(ctx, datastore.NewKey("/zoo/0"), []byte("123")); err != nil {
		t.Fatal(err)
	}
	if err := m.Put(ctx, datastore.NewKey("/rok/1"), []byte("def")); err != nil {
		t.Fatal(err)
	}
	if err := m.Put(ctx, datastore.NewKey("/zoo/1"), []byte("167")); err != nil {
		t.Fatal(err)
	}
	if err := m.Put(ctx, datastore.NewKey("/zoo/2"), []byte("345")); err != nil {
		t.Fatal(err)
	}
	if err := m.Put(ctx, datastore.NewKey("/rok/3"), []byte("abc")); err != nil {
		t.Fatal(err)
	}
	if err := m.Put(ctx, datastore.NewKey("/zoo/3"), []byte("456")); err != nil {
		t.Fatal(err)
	}

	q := query.Query{Limit: 2, Orders: []query.Order{query.OrderByKeyDescending{}}}
	res, err := m.Query(ctx, q)
	if err != nil {
		t.Fatalf("Query fail: %v\n", err)
	}

	entries, err := res.Rest()
	if err != nil {
		t.Fatalf("Query Results.Rest fail: %v\n", err)
	}

	expect := []string{
		"/zoo/3",
		"/zoo/2",
	}

	if len(entries) != len(expect) {
		t.Fatalf("expected %d entries, but got %d", len(expect), len(entries))
	}

	for i, e := range expect {
		if e != entries[i].Key {
			t.Errorf("expected key %s, but got %s", e, entries[i].Key)
		}
	}

	err = res.Close()
	if err != nil {
		t.Errorf("result.Close failed %d", err)
	}
}

func TestQueryLimitAndOffsetAcrossMountsWithSort(t *testing.T) {
	ctx := context.Background()

	mapds1 := sync.MutexWrap(datastore.NewMapDatastore())
	mapds2 := sync.MutexWrap(datastore.NewMapDatastore())
	mapds3 := sync.MutexWrap(datastore.NewMapDatastore())
	m := mount.New([]mount.Mount{
		{Prefix: datastore.NewKey("/rok"), Datastore: mapds1},
		{Prefix: datastore.NewKey("/zoo"), Datastore: mapds2},
		{Prefix: datastore.NewKey("/noop"), Datastore: mapds3},
	})

	if err := m.Put(ctx, datastore.NewKey("/rok/0"), []byte("ghi")); err != nil {
		t.Fatal(err)
	}
	if err := m.Put(ctx, datastore.NewKey("/zoo/0"), []byte("123")); err != nil {
		t.Fatal(err)
	}
	if err := m.Put(ctx, datastore.NewKey("/rok/1"), []byte("def")); err != nil {
		t.Fatal(err)
	}
	if err := m.Put(ctx, datastore.NewKey("/zoo/1"), []byte("167")); err != nil {
		t.Fatal(err)
	}
	if err := m.Put(ctx, datastore.NewKey("/zoo/2"), []byte("345")); err != nil {
		t.Fatal(err)
	}
	if err := m.Put(ctx, datastore.NewKey("/rok/3"), []byte("abc")); err != nil {
		t.Fatal(err)
	}
	if err := m.Put(ctx, datastore.NewKey("/zoo/3"), []byte("456")); err != nil {
		t.Fatal(err)
	}

	q := query.Query{Limit: 3, Offset: 2, Orders: []query.Order{query.OrderByKey{}}}
	res, err := m.Query(ctx, q)
	if err != nil {
		t.Fatalf("Query fail: %v\n", err)
	}

	entries, err := res.Rest()
	if err != nil {
		t.Fatalf("Query Results.Rest fail: %v\n", err)
	}

	expect := []string{
		"/rok/3",
		"/zoo/0",
		"/zoo/1",
	}

	if len(entries) != len(expect) {
		t.Fatalf("expected %d entries, but got %d", len(expect), len(entries))
	}

	for i, e := range expect {
		if e != entries[i].Key {
			t.Errorf("expected key %s, but got %s", e, entries[i].Key)
		}
	}

	err = res.Close()
	if err != nil {
		t.Errorf("result.Close failed %d", err)
	}
}

func TestQueryFilterAcrossMountsWithSort(t *testing.T) {
	ctx := context.Background()

	mapds1 := sync.MutexWrap(datastore.NewMapDatastore())
	mapds2 := sync.MutexWrap(datastore.NewMapDatastore())
	mapds3 := sync.MutexWrap(datastore.NewMapDatastore())
	m := mount.New([]mount.Mount{
		{Prefix: datastore.NewKey("/rok"), Datastore: mapds1},
		{Prefix: datastore.NewKey("/zoo"), Datastore: mapds2},
		{Prefix: datastore.NewKey("/noop"), Datastore: mapds3},
	})

	if err := m.Put(ctx, datastore.NewKey("/rok/0"), []byte("ghi")); err != nil {
		t.Fatal(err)
	}
	if err := m.Put(ctx, datastore.NewKey("/zoo/0"), []byte("123")); err != nil {
		t.Fatal(err)
	}
	if err := m.Put(ctx, datastore.NewKey("/rok/1"), []byte("def")); err != nil {
		t.Fatal(err)
	}
	if err := m.Put(ctx, datastore.NewKey("/zoo/1"), []byte("167")); err != nil {
		t.Fatal(err)
	}
	if err := m.Put(ctx, datastore.NewKey("/zoo/2"), []byte("345")); err != nil {
		t.Fatal(err)
	}
	if err := m.Put(ctx, datastore.NewKey("/rok/3"), []byte("abc")); err != nil {
		t.Fatal(err)
	}
	if err := m.Put(ctx, datastore.NewKey("/zoo/3"), []byte("456")); err != nil {
		t.Fatal(err)
	}

	f := &query.FilterKeyCompare{Op: query.Equal, Key: "/rok/3"}
	q := query.Query{Filters: []query.Filter{f}}
	res, err := m.Query(ctx, q)
	if err != nil {
		t.Fatalf("Query fail: %v\n", err)
	}

	entries, err := res.Rest()
	if err != nil {
		t.Fatalf("Query Results.Rest fail: %v\n", err)
	}

	expect := []string{
		"/rok/3",
	}

	if len(entries) != len(expect) {
		t.Fatalf("expected %d entries, but got %d", len(expect), len(entries))
	}

	for i, e := range expect {
		if e != entries[i].Key {
			t.Errorf("expected key %s, but got %s", e, entries[i].Key)
		}
	}

	err = res.Close()
	if err != nil {
		t.Errorf("result.Close failed %d", err)
	}
}

func TestQueryLimitAndOffsetWithNoData(t *testing.T) {
	ctx := context.Background()

	mapds1 := sync.MutexWrap(datastore.NewMapDatastore())
	mapds2 := sync.MutexWrap(datastore.NewMapDatastore())
	m := mount.New([]mount.Mount{
		{Prefix: datastore.NewKey("/rok"), Datastore: mapds1},
		{Prefix: datastore.NewKey("/zoo"), Datastore: mapds2},
	})

	q := query.Query{Limit: 4, Offset: 3}
	res, err := m.Query(ctx, q)
	if err != nil {
		t.Fatalf("Query fail: %v\n", err)
	}

	entries, err := res.Rest()
	if err != nil {
		t.Fatalf("Query Results.Rest fail: %v\n", err)
	}

	expect := []string{}

	if len(entries) != len(expect) {
		t.Fatalf("expected %d entries, but got %d", len(expect), len(entries))
	}

	err = res.Close()
	if err != nil {
		t.Errorf("result.Close failed %d", err)
	}
}

func TestQueryLimitWithNotEnoughData(t *testing.T) {
	ctx := context.Background()

	mapds1 := sync.MutexWrap(datastore.NewMapDatastore())
	mapds2 := sync.MutexWrap(datastore.NewMapDatastore())
	m := mount.New([]mount.Mount{
		{Prefix: datastore.NewKey("/rok"), Datastore: mapds1},
		{Prefix: datastore.NewKey("/zoo"), Datastore: mapds2},
	})

	if err := m.Put(ctx, datastore.NewKey("/zoo/0"), []byte("123")); err != nil {
		t.Fatal(err)
	}
	if err := m.Put(ctx, datastore.NewKey("/rok/1"), []byte("167")); err != nil {
		t.Fatal(err)
	}

	q := query.Query{Limit: 4}
	res, err := m.Query(ctx, q)
	if err != nil {
		t.Fatalf("Query fail: %v\n", err)
	}

	entries, err := res.Rest()
	if err != nil {
		t.Fatalf("Query Results.Rest fail: %v\n", err)
	}

	expect := []string{
		"/zoo/0",
		"/rok/1",
	}

	if len(entries) != len(expect) {
		t.Fatalf("expected %d entries, but got %d", len(expect), len(entries))
	}

	err = res.Close()
	if err != nil {
		t.Errorf("result.Close failed %d", err)
	}
}

func TestQueryOffsetWithNotEnoughData(t *testing.T) {
	ctx := context.Background()

	mapds1 := sync.MutexWrap(datastore.NewMapDatastore())
	mapds2 := sync.MutexWrap(datastore.NewMapDatastore())
	m := mount.New([]mount.Mount{
		{Prefix: datastore.NewKey("/rok"), Datastore: mapds1},
		{Prefix: datastore.NewKey("/zoo"), Datastore: mapds2},
	})

	if err := m.Put(ctx, datastore.NewKey("/zoo/0"), []byte("123")); err != nil {
		t.Fatal(err)
	}
	if err := m.Put(ctx, datastore.NewKey("/rok/1"), []byte("167")); err != nil {
		t.Fatal(err)
	}

	q := query.Query{Offset: 4}
	res, err := m.Query(ctx, q)
	if err != nil {
		t.Fatalf("Query fail: %v\n", err)
	}

	entries, err := res.Rest()
	if err != nil {
		t.Fatalf("Query Results.Rest fail: %v\n", err)
	}

	expect := []string{}

	if len(entries) != len(expect) {
		t.Fatalf("expected %d entries, but got %d", len(expect), len(entries))
	}

	err = res.Close()
	if err != nil {
		t.Errorf("result.Close failed %d", err)
	}
}

func TestLookupPrio(t *testing.T) {
	ctx := context.Background()

	mapds0 := datastore.NewMapDatastore()
	mapds1 := datastore.NewMapDatastore()

	m := mount.New([]mount.Mount{
		{Prefix: datastore.NewKey("/"), Datastore: mapds0},
		{Prefix: datastore.NewKey("/foo"), Datastore: mapds1},
	})

	if err := m.Put(ctx, datastore.NewKey("/foo/bar"), []byte("123")); err != nil {
		t.Fatal(err)
	}
	if err := m.Put(ctx, datastore.NewKey("/baz"), []byte("234")); err != nil {
		t.Fatal(err)
	}

	found, err := mapds0.Has(ctx, datastore.NewKey("/baz"))
	if err != nil {
		t.Fatalf("Has error: %v", err)
	}
	if g, e := found, true; g != e {
		t.Fatalf("wrong value: %v != %v", g, e)
	}

	found, err = mapds0.Has(ctx, datastore.NewKey("/foo/bar"))
	if err != nil {
		t.Fatalf("Has error: %v", err)
	}
	if g, e := found, false; g != e {
		t.Fatalf("wrong value: %v != %v", g, e)
	}

	found, err = mapds1.Has(ctx, datastore.NewKey("/bar"))
	if err != nil {
		t.Fatalf("Has error: %v", err)
	}
	if g, e := found, true; g != e {
		t.Fatalf("wrong value: %v != %v", g, e)
	}
}

func TestNestedMountSync(t *testing.T) {
	ctx := context.Background()

	internalDSRoot := datastore.NewMapDatastore()
	internalDSFoo := datastore.NewMapDatastore()
	internalDSFooBar := datastore.NewMapDatastore()

	m := mount.New([]mount.Mount{
		{Prefix: datastore.NewKey("/foo"), Datastore: autobatch.NewAutoBatching(internalDSFoo, 10)},
		{Prefix: datastore.NewKey("/foo/bar"), Datastore: autobatch.NewAutoBatching(internalDSFooBar, 10)},
		{Prefix: datastore.NewKey("/"), Datastore: autobatch.NewAutoBatching(internalDSRoot, 10)},
	})

	// Testing scenarios
	// 1) Make sure child(ren) sync
	// 2) Make sure parent syncs
	// 3) Make sure parent only syncs the relevant subtree (instead of fully syncing)

	addToDS := func(str string) {
		t.Helper()
		if err := m.Put(ctx, datastore.NewKey(str), []byte(str)); err != nil {
			t.Fatal(err)
		}
	}

	checkVal := func(d datastore.Datastore, str string, expectFound bool) {
		t.Helper()
		res, err := d.Has(ctx, datastore.NewKey(str))
		if err != nil {
			t.Fatal(err)
		}
		if res != expectFound {
			if expectFound {
				t.Fatal("datastore is missing key")
			}
			t.Fatal("datastore has key it should not have")
		}
	}

	// Add /foo/bar/0, Add /foo/bar/0/1, Add /foo/baz, Add /beep/bop, Sync /foo: all added except last - checks 1 and 2
	addToDS("/foo/bar/0")
	addToDS("/foo/bar/1")
	addToDS("/foo/baz")
	addToDS("/beep/bop")

	if err := m.Sync(ctx, datastore.NewKey("/foo")); err != nil {
		t.Fatal(err)
	}

	checkVal(internalDSFooBar, "/0", true)
	checkVal(internalDSFooBar, "/1", true)
	checkVal(internalDSFoo, "/baz", true)
	checkVal(internalDSRoot, "/beep/bop", false)

	// Add /fwop Add /bloop Sync /fwop, both added - checks 3
	addToDS("/fwop")
	addToDS("/bloop")

	if err := m.Sync(ctx, datastore.NewKey("/fwop")); err != nil {
		t.Fatal(err)
	}

	checkVal(internalDSRoot, "/fwop", true)
	checkVal(internalDSRoot, "/bloop", false)
}

type errQueryDS struct {
	datastore.NullDatastore
}

func (d *errQueryDS) Query(ctx context.Context, q query.Query) (query.Results, error) {
	return nil, errors.New("test error")
}

func TestErrQueryClose(t *testing.T) {
	ctx := context.Background()

	eqds := &errQueryDS{}
	mds := datastore.NewMapDatastore()

	m := mount.New([]mount.Mount{
		{Prefix: datastore.NewKey("/"), Datastore: mds},
		{Prefix: datastore.NewKey("/foo"), Datastore: eqds},
	})

	if err := m.Put(ctx, datastore.NewKey("/baz"), []byte("123")); err != nil {
		t.Fatal(err)
	}

	_, err := m.Query(ctx, query.Query{})
	if err == nil {
		t.Fatal("expected query to fail")
		return
	}
}

func TestMaintenanceFunctions(t *testing.T) {
	ctx := context.Background()

	mapds := dstest.NewTestDatastore(true)
	m := mount.New([]mount.Mount{
		{Prefix: datastore.NewKey("/"), Datastore: mapds},
	})

	if err := m.Check(ctx); err != nil && err.Error() != "checking datastore at /: test error" {
		t.Errorf("Unexpected Check() error: %s", err)
	}

	if err := m.CollectGarbage(ctx); err != nil && err.Error() != "gc on datastore at /: test error" {
		t.Errorf("Unexpected CollectGarbage() error: %s", err)
	}

	if err := m.Scrub(ctx); err != nil && err.Error() != "scrubbing datastore at /: test error" {
		t.Errorf("Unexpected Scrub() error: %s", err)
	}
}

func TestSuite(t *testing.T) {
	mapds0 := datastore.NewMapDatastore()
	mapds1 := datastore.NewMapDatastore()
	mapds2 := datastore.NewMapDatastore()
	mapds3 := datastore.NewMapDatastore()
	m := mount.New([]mount.Mount{
		{Prefix: datastore.NewKey("/prefix"), Datastore: mapds1},
		{Prefix: datastore.NewKey("/prefix/sub"), Datastore: mapds2},
		{Prefix: datastore.NewKey("/0"), Datastore: mapds3},
		{Prefix: datastore.NewKey("/"), Datastore: mapds0},
	})
	dstest.SubtestAll(t, m)
}
