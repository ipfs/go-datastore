package delayed

import (
	"context"
	"testing"
	"time"

	datastore "github.com/ipfs/go-datastore"
	dstest "github.com/ipfs/go-datastore/test"
	delay "github.com/ipfs/go-ipfs-delay"
)

func TestDelayed(t *testing.T) {
	ctx := context.Background()

	d := New(datastore.NewMapDatastore(), delay.Fixed(time.Second))
	now := time.Now()
	k := datastore.NewKey("test")
	err := d.Put(ctx, k, []byte("value"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = d.Get(ctx, k)
	if err != nil {
		t.Fatal(err)
	}
	if time.Since(now) < 2*time.Second {
		t.Fatal("There should have been a delay of 1 second in put and in get")
	}
}

func TestDelayedAll(t *testing.T) {
	// Don't actually delay, we just want to make sure this works correctly, not that it
	// delays anything.
	dstest.SubtestAll(t, New(datastore.NewMapDatastore(), delay.Fixed(0)))
}
