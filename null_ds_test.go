package datastore_test

import (
	"testing"

	"github.com/ipfs/go-datastore"
	dstest "github.com/ipfs/go-datastore/test"
)

func TestNullDatastore(t *testing.T) {
	ds := datastore.NewNullDatastore()
	// The only test that passes. Nothing should be found.
	dstest.SubtestNotFounds(t, ds)
}
