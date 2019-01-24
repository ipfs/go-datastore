package datastore_test

import (
	"testing"

	dstore "github.com/ipfs/go-datastore/v4"
	dstest "github.com/ipfs/go-datastore/v4/test"
)

func TestMapDatastore(t *testing.T) {
	ds := dstore.NewMapDatastore()
	dstest.SubtestAll(t, ds)
}

func TestNullDatastore(t *testing.T) {
	ds := dstore.NewNullDatastore()
	// The only test that passes. Nothing should be found.
	dstest.SubtestNotFounds(t, ds)
}
