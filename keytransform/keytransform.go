package keytransform

import (
	ds "github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
)

// Wrap wraps a given datastore with a KeyTransform function.
// The resulting wrapped datastore will use the transform on all Datastore
// operations.
func Wrap(child ds.Datastore, t KeyTransform) *Datastore {
	if t == nil {
		panic("t (KeyTransform) is nil")
	}

	if child == nil {
		panic("child (ds.Datastore) is nil")
	}

	return &Datastore{child: child, KeyTransform: t}
}

// Datastore keeps a KeyTransform function
type Datastore struct {
	child ds.Datastore

	KeyTransform
}

// Children implements ds.Shim
func (d *Datastore) Children() []ds.Datastore {
	return []ds.Datastore{d.child}
}

// Put stores the given value, transforming the key first.
func (d *Datastore) Put(key ds.Key, value []byte) (err error) {
	return d.child.Put(d.ConvertKey(key), value)
}

// Get returns the value for given key, transforming the key first.
func (d *Datastore) Get(key ds.Key) (value []byte, err error) {
	return d.child.Get(d.ConvertKey(key))
}

// Has returns whether the datastore has a value for a given key, transforming
// the key first.
func (d *Datastore) Has(key ds.Key) (exists bool, err error) {
	return d.child.Has(d.ConvertKey(key))
}

// GetSize returns the size of the value named by the given key, transforming
// the key first.
func (d *Datastore) GetSize(key ds.Key) (size int, err error) {
	return d.child.GetSize(d.ConvertKey(key))
}

// Delete removes the value for given key
func (d *Datastore) Delete(key ds.Key) (err error) {
	return d.child.Delete(d.ConvertKey(key))
}

// Query implements Query, inverting keys on the way back out.
func (d *Datastore) Query(q dsq.Query) (dsq.Results, error) {
	qr, err := d.child.Query(q)
	if err != nil {
		return nil, err
	}

	return dsq.ResultsFromIterator(q, dsq.Iterator{
		Next: func() (dsq.Result, bool) {
			r, ok := qr.NextSync()
			if !ok {
				return r, false
			}
			if r.Error == nil {
				r.Entry.Key = d.InvertKey(ds.RawKey(r.Entry.Key)).String()
			}
			return r, true
		},
		Close: func() error {
			return qr.Close()
		},
	}), nil
}

func (d *Datastore) Close() error {
	return d.child.Close()
}

// DiskUsage implements the PersistentDatastore interface.
func (d *Datastore) DiskUsage() (uint64, error) {
	return ds.DiskUsage(d.child)
}

func (d *Datastore) Batch() (ds.Batch, error) {
	bds, ok := d.child.(ds.Batching)
	if !ok {
		return nil, ds.ErrBatchUnsupported
	}

	childbatch, err := bds.Batch()
	if err != nil {
		return nil, err
	}
	return &transformBatch{
		dst: childbatch,
		f:   d.ConvertKey,
	}, nil
}

type transformBatch struct {
	dst ds.Batch

	f KeyMapping
}

func (t *transformBatch) Put(key ds.Key, val []byte) error {
	return t.dst.Put(t.f(key), val)
}

func (t *transformBatch) Delete(key ds.Key) error {
	return t.dst.Delete(t.f(key))
}

func (t *transformBatch) Commit() error {
	return t.dst.Commit()
}

func (d *Datastore) Check() error {
	if c, ok := d.child.(ds.CheckedDatastore); ok {
		return c.Check()
	}
	return nil
}

func (d *Datastore) Scrub() error {
	if c, ok := d.child.(ds.ScrubbedDatastore); ok {
		return c.Scrub()
	}
	return nil
}

func (d *Datastore) CollectGarbage() error {
	if c, ok := d.child.(ds.GCDatastore); ok {
		return c.CollectGarbage()
	}
	return nil
}

var _ ds.Datastore = (*Datastore)(nil)
var _ ds.GCDatastore = (*Datastore)(nil)
var _ ds.Batching = (*Datastore)(nil)
var _ ds.PersistentDatastore = (*Datastore)(nil)
var _ ds.ScrubbedDatastore = (*Datastore)(nil)
