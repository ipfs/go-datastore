package dstest

import (
	"context"
	"reflect"
	"runtime"
	"testing"

	dstore "github.com/ipfs/go-datastore"
	query "github.com/ipfs/go-datastore/query"
)

// BasicSubtests is a list of all basic tests.
var BasicSubtests = []func(t *testing.T, ds dstore.Datastore){
	SubtestBasicPutGet,
	SubtestNotFounds,
	SubtestCombinations,
	SubtestPrefix,
	SubtestOrder,
	SubtestLimit,
	SubtestFilter,
	SubtestManyKeysAndQuery,
	SubtestReturnSizes,
	SubtestBasicSync,
}

// BatchSubtests is a list of all basic batching datastore tests.
var BatchSubtests = []func(t *testing.T, ds dstore.Batching){
	RunBatchTest,
	RunBatchDeleteTest,
	RunBatchPutAndDeleteTest,
}

func getFunctionName(i interface{}) string {
	return runtime.FuncForPC(reflect.ValueOf(i).Pointer()).Name()
}

func clearDs(t *testing.T, ds dstore.Datastore) {
	ctx := context.Background()
	q, err := ds.Query(ctx, query.Query{KeysOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	res, err := q.Rest()
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range res {
		if err := ds.Delete(ctx, dstore.RawKey(r.Key)); err != nil {
			t.Fatal(err)
		}
	}
}

// SubtestAll tests the given datastore against all the subtests.
func SubtestAll(t *testing.T, ds dstore.Datastore) {
	for _, f := range BasicSubtests {
		t.Run(getFunctionName(f), func(t *testing.T) {
			f(t, ds)
			clearDs(t, ds)
		})
	}
	if ds, ok := ds.(dstore.Batching); ok {
		for _, f := range BatchSubtests {
			t.Run(getFunctionName(f), func(t *testing.T) {
				f(t, ds)
				clearDs(t, ds)
			})
		}
	}
}
