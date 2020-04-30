package fuzzer

import (
	ds "github.com/ipfs/go-datastore"
	prov "github.com/ipfs/go-ds-flatfs"
)

func init() {
	AddOpener("go-ds-flatfs", func(loc string) ds.Datastore {
		d, err := prov.CreateOrOpen(loc, prov.IPFS_DEF_SHARD, false)
		if err != nil {
			panic("could not create db instance")
		}
		return d
	})
}
