package datastore_test

import (
	"io"
	"log"
	"testing"

	"github.com/ipfs/go-datastore"
	dstest "github.com/ipfs/go-datastore/test"
)

func TestMapDatastore(t *testing.T) {
	ds := datastore.NewMapDatastore()
	dstest.SubtestAll(t, ds)
}

func TestLogDatastore(t *testing.T) {
	defer log.SetOutput(log.Writer())
	log.SetOutput(io.Discard)
	ds := datastore.NewLogDatastore(datastore.NewMapDatastore(), "")
	dstest.SubtestAll(t, ds)
}
