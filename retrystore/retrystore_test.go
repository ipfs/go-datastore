package retrystore

import (
	"context"
	"fmt"
	"strings"
	"testing"

	ds "github.com/ipfs/go-datastore"
	failstore "github.com/ipfs/go-datastore/failstore"
)

func TestRetryFailure(t *testing.T) {
	ctx := context.Background()

	myErr := fmt.Errorf("this is an actual error")
	var count int
	fstore := failstore.NewFailstore(ds.NewMapDatastore(), func(op string) error {
		count++
		return myErr
	})

	rds := &Datastore{
		Batching: fstore,
		Retries:  5,
		TempErrFunc: func(err error) bool {
			return err == myErr
		},
	}

	k := ds.NewKey("test")

	_, err := rds.Get(ctx, k)
	if err == nil {
		t.Fatal("expected this to fail")
	}

	if !strings.Contains(err.Error(), "ran out of retries") {
		t.Fatal("got different error than expected: ", err)
	}

	if count != 6 {
		t.Fatal("expected five retries (six executions), got: ", count)
	}
}

func TestRealErrorGetsThrough(t *testing.T) {
	ctx := context.Background()

	myErr := fmt.Errorf("this is an actual error")
	fstore := failstore.NewFailstore(ds.NewMapDatastore(), func(op string) error {
		return myErr
	})

	rds := &Datastore{
		Batching: fstore,
		Retries:  5,
		TempErrFunc: func(err error) bool {
			return false
		},
	}

	k := ds.NewKey("test")
	_, err := rds.Get(ctx, k)
	if err != myErr {
		t.Fatal("expected my own error")
	}

	_, err = rds.Has(ctx, k)
	if err != myErr {
		t.Fatal("expected my own error")
	}

	err = rds.Put(ctx, k, nil)
	if err != myErr {
		t.Fatal("expected my own error")
	}
}

func TestRealErrorAfterTemp(t *testing.T) {
	ctx := context.Background()

	myErr := fmt.Errorf("this is an actual error")
	tempErr := fmt.Errorf("this is a temp error")
	var count int
	fstore := failstore.NewFailstore(ds.NewMapDatastore(), func(op string) error {
		count++
		if count < 3 {
			return tempErr
		}

		return myErr
	})

	rds := &Datastore{
		Batching: fstore,
		Retries:  5,
		TempErrFunc: func(err error) bool {
			return err == tempErr
		},
	}

	k := ds.NewKey("test")
	_, err := rds.Get(ctx, k)
	if err != myErr {
		t.Fatal("expected my own error")
	}
}

func TestSuccessAfterTemp(t *testing.T) {
	ctx := context.Background()

	tempErr := fmt.Errorf("this is a temp error")
	var count int
	fstore := failstore.NewFailstore(ds.NewMapDatastore(), func(op string) error {
		count++
		if count < 3 {
			return tempErr
		}
		count = 0
		return nil
	})

	rds := &Datastore{
		Batching: fstore,
		Retries:  5,
		TempErrFunc: func(err error) bool {
			return err == tempErr
		},
	}

	k := ds.NewKey("test")
	val := []byte("foo")

	err := rds.Put(ctx, k, val)
	if err != nil {
		t.Fatal(err)
	}

	has, err := rds.Has(ctx, k)
	if err != nil {
		t.Fatal(err)
	}

	if !has {
		t.Fatal("should have this thing")
	}

	out, err := rds.Get(ctx, k)
	if err != nil {
		t.Fatal(err)
	}

	if string(out) != string(val) {
		t.Fatal("got wrong value")
	}
}
