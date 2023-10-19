package keytransform

import (
	"context"

	ds "github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
)

// WrapTxnDatastore wraps a given datastore with a KeyTransform function.
// The resulting wrapped datastore will use the transform on all TxnDatastore
// operations.
func WrapTxnDatastore(child ds.TxnDatastore, t KeyTransform) *TxnDatastore {
	if t == nil {
		panic("t (KeyTransform) is nil")
	}

	if child == nil {
		panic("child (ds.TxnDatastore) is nil")
	}

	return &TxnDatastore{ds: Wrap(child, t), child: child, KeyTransform: t}
}

// TxnDatastore keeps a KeyTransform function
type TxnDatastore struct {
	ds    *Datastore
	child ds.TxnDatastore

	KeyTransform
}

var _ ds.Datastore = (*TxnDatastore)(nil)
var _ ds.Batching = (*TxnDatastore)(nil)
var _ ds.Shim = (*TxnDatastore)(nil)
var _ ds.PersistentDatastore = (*TxnDatastore)(nil)
var _ ds.CheckedDatastore = (*TxnDatastore)(nil)
var _ ds.ScrubbedDatastore = (*TxnDatastore)(nil)
var _ ds.GCDatastore = (*TxnDatastore)(nil)
var _ ds.TxnDatastore = (*TxnDatastore)(nil)

// Children implements ds.Shim
func (d *TxnDatastore) Children() []ds.Datastore {
	return []ds.Datastore{d.child}
}

// Put stores the given value, transforming the key first.
func (d *TxnDatastore) Put(ctx context.Context, key ds.Key, value []byte) (err error) {
	return d.ds.Put(ctx, d.ConvertKey(key), value)
}

// Sync implements Datastore.Sync
func (d *TxnDatastore) Sync(ctx context.Context, prefix ds.Key) error {
	return d.ds.Sync(ctx, d.ConvertKey(prefix))
}

// Get returns the value for given key, transforming the key first.
func (d *TxnDatastore) Get(ctx context.Context, key ds.Key) (value []byte, err error) {
	return d.ds.Get(ctx, d.ConvertKey(key))
}

// Has returns whether the datastore has a value for a given key, transforming
// the key first.
func (d *TxnDatastore) Has(ctx context.Context, key ds.Key) (exists bool, err error) {
	return d.ds.Has(ctx, d.ConvertKey(key))
}

// GetSize returns the size of the value named by the given key, transforming
// the key first.
func (d *TxnDatastore) GetSize(ctx context.Context, key ds.Key) (size int, err error) {
	return d.ds.GetSize(ctx, d.ConvertKey(key))
}

// Delete removes the value for given key
func (d *TxnDatastore) Delete(ctx context.Context, key ds.Key) (err error) {
	return d.ds.Delete(ctx, d.ConvertKey(key))
}

// Query implements Query, inverting keys on the way back out.
func (d *TxnDatastore) Query(ctx context.Context, q dsq.Query) (dsq.Results, error) {
	return d.ds.Query(ctx, q)
}

func (d *TxnDatastore) Close() error {
	return d.ds.Close()
}

// DiskUsage implements the PersistentTxnDatastore interface.
func (d *TxnDatastore) DiskUsage(ctx context.Context) (uint64, error) {
	return d.ds.DiskUsage(ctx)
}

func (d *TxnDatastore) Batch(ctx context.Context) (ds.Batch, error) {
	return d.ds.Batch(ctx)
}

func (d *TxnDatastore) Check(ctx context.Context) error {
	return d.ds.Check(ctx)
}

func (d *TxnDatastore) Scrub(ctx context.Context) error {
	return d.ds.Scrub(ctx)
}

func (d *TxnDatastore) CollectGarbage(ctx context.Context) error {
	return d.ds.CollectGarbage(ctx)
}

func (d *TxnDatastore) NewTransaction(ctx context.Context, readOnly bool) (ds.Txn, error) {
	childTxn, err := d.child.NewTransaction(ctx, readOnly)
	if err != nil {
		return nil, err
	}
	return &txnWrapper{child: childTxn, KeyTransform: d.KeyTransform}, nil
}

type txnWrapper struct {
	child ds.Txn

	KeyTransform
}

var _ ds.Txn = (*txnWrapper)(nil)

func (t *txnWrapper) Get(ctx context.Context, key ds.Key) (value []byte, err error) {
	return t.child.Get(ctx, t.ConvertKey(key))
}

func (t *txnWrapper) Has(ctx context.Context, key ds.Key) (exists bool, err error) {
	return t.child.Has(ctx, t.ConvertKey(key))
}

func (t *txnWrapper) GetSize(ctx context.Context, key ds.Key) (size int, err error) {
	return t.child.GetSize(ctx, t.ConvertKey(key))
}

func (t *txnWrapper) Query(ctx context.Context, q dsq.Query) (dsq.Results, error) {
	nq, cq := t.prepareQuery(q)

	cqr, err := t.child.Query(ctx, cq)
	if err != nil {
		return nil, err
	}

	qr := dsq.ResultsFromIterator(q, dsq.Iterator{
		Next: func() (dsq.Result, bool) {
			r, ok := cqr.NextSync()
			if !ok {
				return r, false
			}
			if r.Error == nil {
				r.Entry.Key = t.InvertKey(ds.RawKey(r.Entry.Key)).String()
			}
			return r, true
		},
		Close: func() error {
			return cqr.Close()
		},
	})
	return dsq.NaiveQueryApply(nq, qr), nil
}

// Split the query into a child query and a naive query. That way, we can make
// the child datastore do as much work as possible.
func (t *txnWrapper) prepareQuery(q dsq.Query) (naive, child dsq.Query) {

	// First, put everything in the child query. Then, start taking things
	// out.
	child = q

	// Always let the child handle the key prefix.
	child.Prefix = t.ConvertKey(ds.NewKey(child.Prefix)).String()

	// Check if the key transform is order-preserving so we can use the
	// child datastore's built-in ordering.
	orderPreserving := false
	switch t.KeyTransform.(type) {
	case PrefixTransform, *PrefixTransform:
		orderPreserving = true
	}

	// Try to let the child handle ordering.
orders:
	for i, o := range child.Orders {
		switch o.(type) {
		case dsq.OrderByValue, *dsq.OrderByValue,
			dsq.OrderByValueDescending, *dsq.OrderByValueDescending:
			// Key doesn't matter.
			continue
		case dsq.OrderByKey, *dsq.OrderByKey,
			dsq.OrderByKeyDescending, *dsq.OrderByKeyDescending:
			// if the key transform preserves order, we can delegate
			// to the child datastore.
			if orderPreserving {
				// When sorting, we compare with the first
				// Order, then, if equal, we compare with the
				// second Order, etc. However, keys are _unique_
				// so we'll never apply any additional orders
				// after ordering by key.
				child.Orders = child.Orders[:i+1]
				break orders
			}
		}

		// Can't handle this order under transform, punt it to a naive
		// ordering.
		naive.Orders = q.Orders
		child.Orders = nil
		naive.Offset = q.Offset
		child.Offset = 0
		naive.Limit = q.Limit
		child.Limit = 0
		break
	}

	// Try to let the child handle the filters.

	// don't modify the original filters.
	child.Filters = append([]dsq.Filter(nil), child.Filters...)

	for i, f := range child.Filters {
		switch f := f.(type) {
		case dsq.FilterValueCompare, *dsq.FilterValueCompare:
			continue
		case dsq.FilterKeyCompare:
			child.Filters[i] = dsq.FilterKeyCompare{
				Op:  f.Op,
				Key: t.ConvertKey(ds.NewKey(f.Key)).String(),
			}
			continue
		case *dsq.FilterKeyCompare:
			child.Filters[i] = &dsq.FilterKeyCompare{
				Op:  f.Op,
				Key: t.ConvertKey(ds.NewKey(f.Key)).String(),
			}
			continue
		case dsq.FilterKeyPrefix:
			child.Filters[i] = dsq.FilterKeyPrefix{
				Prefix: t.ConvertKey(ds.NewKey(f.Prefix)).String(),
			}
			continue
		case *dsq.FilterKeyPrefix:
			child.Filters[i] = &dsq.FilterKeyPrefix{
				Prefix: t.ConvertKey(ds.NewKey(f.Prefix)).String(),
			}
			continue
		}

		// Not a known filter, defer to the naive implementation.
		naive.Filters = q.Filters
		child.Filters = nil
		naive.Offset = q.Offset
		child.Offset = 0
		naive.Limit = q.Limit
		child.Limit = 0
		break
	}
	return
}

func (t txnWrapper) Put(ctx context.Context, key ds.Key, value []byte) error {
	return t.child.Put(ctx, t.ConvertKey(key), value)
}

func (t txnWrapper) Delete(ctx context.Context, key ds.Key) error {
	return t.child.Delete(ctx, t.ConvertKey(key))
}

func (t txnWrapper) Commit(ctx context.Context) error {
	return t.child.Commit(ctx)
}

func (t txnWrapper) Discard(ctx context.Context) {
	t.child.Discard(ctx)
}
