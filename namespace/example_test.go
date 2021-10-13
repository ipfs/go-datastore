package namespace_test

import (
	"context"
	"fmt"

	ds "github.com/ipfs/go-datastore"
	nsds "github.com/ipfs/go-datastore/namespace"
)

func Example() {
	ctx := context.Background()

	mp := ds.NewMapDatastore()
	ns := nsds.Wrap(mp, ds.NewKey("/foo/bar"))

	k := ds.NewKey("/beep")
	v := "boop"

	if err := ns.Put(ctx, k, []byte(v)); err != nil {
		panic(err)
	}
	fmt.Printf("ns.Put %s %s\n", k, v)

	v2, _ := ns.Get(ctx, k)
	fmt.Printf("ns.Get %s -> %s\n", k, v2)

	k3 := ds.NewKey("/foo/bar/beep")
	v3, _ := mp.Get(ctx, k3)
	fmt.Printf("mp.Get %s -> %s\n", k3, v3)
	// Output:
	// ns.Put /beep boop
	// ns.Get /beep -> boop
	// mp.Get /foo/bar/beep -> boop
}
