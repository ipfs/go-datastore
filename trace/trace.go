// Package trace wraps a datastore where all datastore interactions are traced
// with open telemetry.
package trace

import (
	"context"
	"fmt"
	"io"

	ds "github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
	"go.opentelemetry.io/otel/attribute"
	otel "go.opentelemetry.io/otel/trace"
)

// New returns a new traced datastore. All datastore interactions are traced.
func New(ds ds.Datastore, tracer otel.Tracer) *Datastore {
	return &Datastore{ds: ds, tracer: tracer}
}

// Datastore is an adapter that traces inner datastore interactions.
type Datastore struct {
	ds     ds.Datastore
	tracer otel.Tracer
}

var (
	_ ds.Datastore           = (*Datastore)(nil)
	_ ds.Batching            = (*Datastore)(nil)
	_ ds.PersistentDatastore = (*Datastore)(nil)
	_ ds.TxnDatastore        = (*Datastore)(nil)
	_ ds.CheckedDatastore    = (*Datastore)(nil)
	_ ds.ScrubbedDatastore   = (*Datastore)(nil)
	_ ds.GCDatastore         = (*Datastore)(nil)
	_ io.Closer              = (*Datastore)(nil)
)

// Put implements the ds.Datastore interface.
func (t *Datastore) Put(ctx context.Context, key ds.Key, value []byte) (err error) {
	ctx, span := t.tracer.Start(ctx, "Put", otel.WithAttributes(attribute.String("key", key.String())))
	defer span.End()
	return t.ds.Put(ctx, key, value)
}

// Sync implements Datastore.Sync
func (t *Datastore) Sync(ctx context.Context, key ds.Key) error {
	ctx, span := t.tracer.Start(ctx, "Sync", otel.WithAttributes(attribute.String("key", key.String())))
	defer span.End()
	return t.ds.Sync(ctx, key)
}

// Get implements the ds.Datastore interface.
func (t *Datastore) Get(ctx context.Context, key ds.Key) (value []byte, err error) {
	ctx, span := t.tracer.Start(ctx, "Get", otel.WithAttributes(attribute.String("key", key.String())))
	defer span.End()
	return t.ds.Get(ctx, key)
}

// Has implements the ds.Datastore interface.
func (t *Datastore) Has(ctx context.Context, key ds.Key) (exists bool, err error) {
	ctx, span := t.tracer.Start(ctx, "Has", otel.WithAttributes(attribute.String("key", key.String())))
	defer span.End()
	return t.ds.Has(ctx, key)
}

// GetSize implements the ds.Datastore interface.
func (t *Datastore) GetSize(ctx context.Context, key ds.Key) (size int, err error) {
	ctx, span := t.tracer.Start(ctx, "GetSize", otel.WithAttributes(attribute.String("key", key.String())))
	defer span.End()
	return t.ds.GetSize(ctx, key)
}

// Delete implements the ds.Datastore interface.
func (t *Datastore) Delete(ctx context.Context, key ds.Key) (err error) {
	ctx, span := t.tracer.Start(ctx, "Delete", otel.WithAttributes(attribute.String("key", key.String())))
	defer span.End()
	return t.ds.Delete(ctx, key)
}

// Query implements the ds.Datastore interface.
func (t *Datastore) Query(ctx context.Context, q dsq.Query) (dsq.Results, error) {
	ctx, span := t.tracer.Start(ctx, "Query", otel.WithAttributes(attribute.String("query", q.String())))
	defer span.End()
	return t.ds.Query(ctx, q)
}

// Batch implements the ds.Batching interface.
func (t *Datastore) Batch(ctx context.Context) (ds.Batch, error) {
	ctx, span := t.tracer.Start(ctx, "Batch")
	defer span.End()

	if batch, ok := t.ds.(ds.Batching); ok {
		return batch.Batch(ctx)
	}

	return ds.NewBasicBatch(t), nil
}

// DiskUsage implements the ds.PersistentDatastore interface.
func (t *Datastore) DiskUsage(ctx context.Context) (uint64, error) {
	ctx, span := t.tracer.Start(ctx, "DiskUsage")
	defer span.End()
	return ds.DiskUsage(ctx, t.ds)
}

// Scrub implements the ds.ScrubbedDatastore interface.
func (t *Datastore) Scrub(ctx context.Context) error {
	ctx, span := t.tracer.Start(ctx, "Scrub")
	defer span.End()

	if dstore, ok := t.tracer.(ds.ScrubbedDatastore); ok {
		return dstore.Scrub(ctx)
	}

	return nil
}

// CollectGarbage implements the ds.GCDatastore interface.
func (t *Datastore) CollectGarbage(ctx context.Context) error {
	ctx, span := t.tracer.Start(ctx, "CollectGarbage")
	defer span.End()

	if dstore, ok := t.tracer.(ds.GCDatastore); ok {
		return dstore.CollectGarbage(ctx)
	}

	return nil
}

// Check implements the ds.CheckedDatastore interface.
func (t *Datastore) Check(ctx context.Context) error {
	ctx, span := t.tracer.Start(ctx, "Check")
	defer span.End()

	if dstore, ok := t.tracer.(ds.CheckedDatastore); ok {
		return dstore.Check(ctx)
	}

	return nil
}

// NewTransaction implements the ds.TxnDatastore interface.
func (t *Datastore) NewTransaction(ctx context.Context, readOnly bool) (ds.Txn, error) {
	ctx, span := t.tracer.Start(ctx, "NewTransaction", otel.WithAttributes(attribute.Bool("readOnly", readOnly)))
	defer span.End()

	if txnDs, ok := t.ds.(ds.TxnDatastore); ok {
		txn, err := txnDs.NewTransaction(ctx, readOnly)
		if err != nil {
			return nil, err
		}
		return &Txn{txn: txn, tracer: t.tracer}, nil
	}

	return nil, fmt.Errorf("transactions are unsupported by traced datastore")
}

// Close closes the inner datastore (if it implements the io.Closer interface).
func (t *Datastore) Close() error {
	if closer, ok := t.ds.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

// Txn is an adapter that traces datastore transactions
type Txn struct {
	txn    ds.Txn
	tracer otel.Tracer
}

var _ ds.Txn = (*Txn)(nil)

// Put implements the ds.Txn interface.
func (t *Txn) Put(ctx context.Context, key ds.Key, value []byte) (err error) {
	ctx, span := t.tracer.Start(ctx, "Put", otel.WithAttributes(attribute.String("key", key.String())))
	defer span.End()
	return t.txn.Put(ctx, key, value)
}

// Get implements the ds.Txn interface.
func (t *Txn) Get(ctx context.Context, key ds.Key) (value []byte, err error) {
	ctx, span := t.tracer.Start(ctx, "Get", otel.WithAttributes(attribute.String("key", key.String())))
	defer span.End()
	return t.txn.Get(ctx, key)
}

// Has implements the ds.Txn interface.
func (t *Txn) Has(ctx context.Context, key ds.Key) (exists bool, err error) {
	ctx, span := t.tracer.Start(ctx, "Has", otel.WithAttributes(attribute.String("key", key.String())))
	defer span.End()
	return t.txn.Has(ctx, key)
}

// GetSize implements the ds.Txn interface.
func (t *Txn) GetSize(ctx context.Context, key ds.Key) (size int, err error) {
	ctx, span := t.tracer.Start(ctx, "GetSize", otel.WithAttributes(attribute.String("key", key.String())))
	defer span.End()
	return t.txn.GetSize(ctx, key)
}

// Delete implements the ds.Txn interface.
func (t *Txn) Delete(ctx context.Context, key ds.Key) (err error) {
	ctx, span := t.tracer.Start(ctx, "Delete", otel.WithAttributes(attribute.String("key", key.String())))
	defer span.End()
	return t.txn.Delete(ctx, key)
}

// Query implements the ds.Txn interface.
func (t *Txn) Query(ctx context.Context, q dsq.Query) (dsq.Results, error) {
	ctx, span := t.tracer.Start(ctx, "Query", otel.WithAttributes(attribute.String("query", q.String())))
	defer span.End()
	return t.txn.Query(ctx, q)
}

// Commit implements the ds.Txn interface.
func (t *Txn) Commit(ctx context.Context) error {
	ctx, span := t.tracer.Start(ctx, "Commit")
	defer span.End()
	return t.txn.Commit(ctx)
}

// Discard implements the ds.Txn interface.
func (t *Txn) Discard(ctx context.Context) {
	ctx, span := t.tracer.Start(ctx, "Discard")
	defer span.End()
	t.txn.Discard(ctx)
}
