// Package failstore implements a datastore which can produce
// custom failures on operations by calling a user-provided
// error function.
package failstore

import (
	"context"

	ds "github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
)

// Failstore is a datastore which fails according to a user-provided
// function.
type Failstore struct {
	child   ds.Datastore
	errfunc func(string) error
}

// NewFailstore creates a new datastore with the given error function.
// The efunc will be called with different strings depending on the
// datastore function: put, get, has, delete, query, batch, batch-put,
// batch-delete and batch-commit are the possible values.
func NewFailstore(c ds.Datastore, efunc func(string) error) *Failstore {
	return &Failstore{
		child:   c,
		errfunc: efunc,
	}
}

// Put puts a key/value into the datastore.
func (d *Failstore) Put(ctx context.Context, k ds.Key, val []byte) error {
	err := d.errfunc("put")
	if err != nil {
		return err
	}

	return d.child.Put(ctx, k, val)
}

// Sync implements Datastore.Sync
func (d *Failstore) Sync(ctx context.Context, prefix ds.Key) error {
	err := d.errfunc("sync")
	if err != nil {
		return err
	}

	return d.child.Sync(ctx, prefix)
}

// Get retrieves a value from the datastore.
func (d *Failstore) Get(ctx context.Context, k ds.Key) ([]byte, error) {
	err := d.errfunc("get")
	if err != nil {
		return nil, err
	}

	return d.child.Get(ctx, k)
}

// Has returns if the datastore contains a key/value.
func (d *Failstore) Has(ctx context.Context, k ds.Key) (bool, error) {
	err := d.errfunc("has")
	if err != nil {
		return false, err
	}

	return d.child.Has(ctx, k)
}

// GetSize returns the size of the value in the datastore, if present.
func (d *Failstore) GetSize(ctx context.Context, k ds.Key) (int, error) {
	err := d.errfunc("getsize")
	if err != nil {
		return -1, err
	}

	return d.child.GetSize(ctx, k)
}

// Delete removes a key/value from the datastore.
func (d *Failstore) Delete(ctx context.Context, k ds.Key) error {
	err := d.errfunc("delete")
	if err != nil {
		return err
	}

	return d.child.Delete(ctx, k)
}

// Query performs a query on the datastore.
func (d *Failstore) Query(ctx context.Context, q dsq.Query) (dsq.Results, error) {
	err := d.errfunc("query")
	if err != nil {
		return nil, err
	}

	return d.child.Query(ctx, q)
}

// DiskUsage implements the PersistentDatastore interface.
func (d *Failstore) DiskUsage() (uint64, error) {
	if err := d.errfunc("disk-usage"); err != nil {
		return 0, err
	}
	return ds.DiskUsage(d.child)
}

//  Close implements the Datastore interface
func (d *Failstore) Close() error {
	return d.child.Close()
}

// FailBatch implements batching operations on the Failstore.
type FailBatch struct {
	cb     ds.Batch
	dstore *Failstore
}

// Batch returns a new Batch Failstore.
func (d *Failstore) Batch() (ds.Batch, error) {
	if err := d.errfunc("batch"); err != nil {
		return nil, err
	}

	b, err := d.child.(ds.Batching).Batch()
	if err != nil {
		return nil, err
	}

	return &FailBatch{
		cb:     b,
		dstore: d,
	}, nil
}

// Put does a batch put.
func (b *FailBatch) Put(ctx context.Context, k ds.Key, val []byte) error {
	if err := b.dstore.errfunc("batch-put"); err != nil {
		return err
	}

	return b.cb.Put(ctx, k, val)
}

// Delete does a batch delete.
func (b *FailBatch) Delete(ctx context.Context, k ds.Key) error {
	if err := b.dstore.errfunc("batch-delete"); err != nil {
		return err
	}

	return b.cb.Delete(ctx, k)
}

// Commit commits all operations in the batch.
func (b *FailBatch) Commit(ctx context.Context) error {
	if err := b.dstore.errfunc("batch-commit"); err != nil {
		return err
	}

	return b.cb.Commit(ctx)
}
