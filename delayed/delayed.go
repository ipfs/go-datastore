// Package delayed wraps a datastore allowing to artificially
// delay all operations.
package delayed

import (
	"context"
	"io"

	ds "github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
	delay "github.com/ipfs/go-ipfs-delay"
)

// New returns a new delayed datastore.
func New(ds ds.Datastore, delay delay.D) *Delayed {
	return &Delayed{ds: ds, delay: delay}
}

// Delayed is an adapter that delays operations on the inner datastore.
type Delayed struct {
	ds    ds.Datastore
	delay delay.D
}

var _ ds.Batching = (*Delayed)(nil)
var _ io.Closer = (*Delayed)(nil)

// Put implements the ds.Datastore interface.
func (dds *Delayed) Put(ctx context.Context, key ds.Key, value []byte) (err error) {
	dds.delay.Wait()
	return dds.ds.Put(ctx, key, value)
}

// Sync implements Datastore.Sync
func (dds *Delayed) Sync(ctx context.Context, prefix ds.Key) error {
	dds.delay.Wait()
	return dds.ds.Sync(ctx, prefix)
}

// Get implements the ds.Datastore interface.
func (dds *Delayed) Get(ctx context.Context, key ds.Key) (value []byte, err error) {
	dds.delay.Wait()
	return dds.ds.Get(ctx, key)
}

// Has implements the ds.Datastore interface.
func (dds *Delayed) Has(ctx context.Context, key ds.Key) (exists bool, err error) {
	dds.delay.Wait()
	return dds.ds.Has(ctx, key)
}

// GetSize implements the ds.Datastore interface.
func (dds *Delayed) GetSize(ctx context.Context, key ds.Key) (size int, err error) {
	dds.delay.Wait()
	return dds.ds.GetSize(ctx, key)
}

// Delete implements the ds.Datastore interface.
func (dds *Delayed) Delete(ctx context.Context, key ds.Key) (err error) {
	dds.delay.Wait()
	return dds.ds.Delete(ctx, key)
}

// Query implements the ds.Datastore interface.
func (dds *Delayed) Query(ctx context.Context, q dsq.Query) (dsq.Results, error) {
	dds.delay.Wait()
	return dds.ds.Query(ctx, q)
}

// Batch implements the ds.Batching interface.
func (dds *Delayed) Batch(ctx context.Context) (ds.Batch, error) {
	return ds.NewBasicBatch(dds), nil
}

// DiskUsage implements the ds.PersistentDatastore interface.
func (dds *Delayed) DiskUsage(ctx context.Context) (uint64, error) {
	dds.delay.Wait()
	return ds.DiskUsage(ctx, dds.ds)
}

// Close closes the inner datastore (if it implements the io.Closer interface).
func (dds *Delayed) Close() error {
	if closer, ok := dds.ds.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}
