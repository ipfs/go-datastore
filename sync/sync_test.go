package sync

import (
	"testing"

	ds "github.com/ipfs/go-datastore/v4"
	dstest "github.com/ipfs/go-datastore/v4/test"
)

func TestSync(t *testing.T) {
	dstest.SubtestAll(t, MutexWrap(ds.NewMapDatastore()))
}
