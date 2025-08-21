package contextds

import (
	"context"
	"fmt"

	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
)

var _ datastore.Datastore = (*Datastore)(nil)
var _ datastore.Batching = (*Datastore)(nil)
var _ datastore.TxnDatastore = (*Datastore)(nil)

// WrapsDatastore wraps around the given datastore.Datastore making its operations context-aware
// It intercepts datastore operations routing them to the current Write or Read if one exists on the context.
func WrapDatastore(ds datastore.Datastore) datastore.Datastore {
	return &Datastore{
		inner: ds,
	}
}

// Datastore is a wrapper around a datastore.Datastore that provides context-aware operations.
// See [WrapDatastore].
type Datastore struct {
	inner datastore.Datastore
}

func (ds *Datastore) Put(ctx context.Context, key datastore.Key, value []byte) error {
	if write, ok := GetWrite(ctx); ok {
		return write.Put(ctx, key, value)
	}
	return ds.inner.Put(ctx, key, value)
}

func (ds *Datastore) Delete(ctx context.Context, key datastore.Key) error {
	if write, ok := GetWrite(ctx); ok {
		return write.Delete(ctx, key)
	}
	return ds.inner.Delete(ctx, key)
}

func (ds *Datastore) Get(ctx context.Context, key datastore.Key) ([]byte, error) {
	if read, ok := GetRead(ctx); ok {
		return read.Get(ctx, key)
	}
	return ds.inner.Get(ctx, key)
}

func (ds *Datastore) Has(ctx context.Context, key datastore.Key) (bool, error) {
	if read, ok := GetRead(ctx); ok {
		return read.Has(ctx, key)
	}
	return ds.inner.Has(ctx, key)
}

func (ds *Datastore) GetSize(ctx context.Context, key datastore.Key) (int, error) {
	if read, ok := GetRead(ctx); ok {
		return read.GetSize(ctx, key)
	}
	return ds.inner.GetSize(ctx, key)
}

func (ds *Datastore) Query(ctx context.Context, q query.Query) (query.Results, error) {
	if read, ok := GetRead(ctx); ok {
		return read.Query(ctx, q)
	}
	return ds.inner.Query(ctx, q)
}

func (ds *Datastore) Close() error {
	return ds.inner.Close()
}

func (ds *Datastore) Sync(ctx context.Context, prefix datastore.Key) error {
	return ds.inner.Sync(ctx, prefix)
}

func (ds *Datastore) Batch(ctx context.Context) (datastore.Batch, error) {
	bds, ok := ds.inner.(datastore.Batching)
	if !ok {
		return nil, datastore.ErrBatchUnsupported
	}

	return bds.Batch(ctx)
}

func (ds *Datastore) NewTransaction(ctx context.Context, readOnly bool) (datastore.Txn, error) {
	tds, ok := ds.inner.(datastore.TxnDatastore)
	if !ok {
		return nil, fmt.Errorf("transactions not supported")
	}

	return tds.NewTransaction(ctx, readOnly)
}
