package table

import (
	"testing"

	"github.com/jbenet/go-datastore/Godeps/_workspace/src/github.com/syndtr/goleveldb/leveldb/testutil"
)

func TestTable(t *testing.T) {
	testutil.RunSuite(t, "Table Suite")
}
