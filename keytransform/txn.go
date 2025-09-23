package keytransform

import (
	"context"

	ds "github.com/ipfs/go-datastore"
)

var _ ds.TxnDatastore = (*TxnKeyTransformDatastore)(nil)

// All methods are provided by embedded *Datastore except NewTransaction.
type TxnKeyTransformDatastore struct {
	*Datastore
	txnChild ds.TxnDatastore
	t        KeyTransform
}

func WrapTxn(child ds.TxnDatastore, t KeyTransform) ds.TxnDatastore {
	kt := Wrap(child, t)

	return &TxnKeyTransformDatastore{
		Datastore: kt,
		txnChild:  child,
		t:         t,
	}
}

// NewTransaction returns a Txn that runs all Read/Write operations through a key transform datastore
// backed by the original txn.
func (txnktds *TxnKeyTransformDatastore) NewTransaction(ctx context.Context, readOnly bool) (ds.Txn, error) {
	// New transaction - normal
	txn, err := txnktds.txnChild.NewTransaction(ctx, readOnly)
	if err != nil {
		return nil, err
	}

	// wrap it in an object that augments it to be a Datastore
	txnKt := &wrappedTxn{
		Txn: txn,
	}

	// Make that object a key-transform datastore with the original KeyTransform.
	kt := Wrap(txnKt, txnktds.t)

	// Finally, ensure that this datastore provides Commit and Discard() so that it can be
	// a transaction.
	return &wrappedKt{
		Datastore: kt,
		txn:       txnKt,
	}, nil
}

// makes a txn a datastore
type wrappedTxn struct {
	ds.Txn
}

func (wtxn *wrappedTxn) Close() error {
	return nil
}

func (wtxn *wrappedTxn) Sync(context.Context, ds.Key) error {
	return nil
}

// makes a datastore a tnx
type wrappedKt struct {
	*Datastore
	txn ds.Txn
}

func (wkt *wrappedKt) Commit(ctx context.Context) error {
	return wkt.txn.Commit(ctx)
}

func (wkt *wrappedKt) Discard(ctx context.Context) {
	wkt.txn.Discard(ctx)
}
