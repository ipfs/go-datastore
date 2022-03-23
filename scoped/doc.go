// Package scoped introduces a Datastore Shim that scopes down a source datastore
// to the features supported by a target datastore. This is useful e.g. for dispatching
// datastores, where the dispatcher needs to dynamically implement the same features
// as the dispatchee, without knowing them statically.
//
// Use the Wrap function to wrap a datastore so that its interface is scoped down to
// only those features supported both by it and its target datastore. Note that this
// is a set intersection--if the target implements a feature not supported by the
// wrapped datastore, then the resulting shim will not implement them either.
//
// For example:
//
//  import (
//    "context"
//    scopedds "github.com/ipfs/go-datastore/scoped"
//    ds "github.com/ipfs/go-datastore"
//  )
//
//  type BatchingDS struct { ds.Datastore }
//
//  func (b *BatchingDS) Batch(ctx context.Context) (ds.Batch, error) {
//    // custom batching
//    return nil, nil
//  }
//
//  type BoringDS struct { ds.Datastore }
//
//  func Dispatcher(dstore ds.Datastore) ds.Datastore {
//    dispatcher := &BatchingDS{Datastore: dstore}
//    dispatchee := &BoringDS{Datastore: dstore}
//
//    // the dispatcher supports batching, but since the dispatchee
//    // doesn't, the returned dispatcher does NOT implement ds.Batching
//
//    return scoped.Wrap(dispatcher, dispatchee)
//  }

package scoped

//go:generate go run generate/main.go
