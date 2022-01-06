// Package lrustore provides a datastore wrapper that limits the number of
// items stored to the specified capacity. When at capacity, the oldest items
// are removed in LRU to make root for new items.  Putting and getting items
// maintains LRU order.
package lrustore

import (
	"context"
	"fmt"
	"sync"

	lru "github.com/golang/groupcache/lru"
	ds "github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
)

// LRUStore is a LRU-cache that stores keys in memory and values in a datastore.
type LRUStore struct {
	dstore ds.Datastore
	lru    *lru.Cache
	lock   sync.RWMutex
	rmErr  error
	rmCtx  context.Context
}

var _ ds.Datastore = (*LRUStore)(nil)
var _ ds.Batching = (*LRUStore)(nil)
var _ ds.PersistentDatastore = (*LRUStore)(nil)

// New creates a new LRUStore instance.  The context is only used cancel
// a call to this function while it is accessing the data store.
func New(ctx context.Context, dstore ds.Datastore, capacity int) (*LRUStore, error) {
	// Create LRU cache that deletes value from datastore when key is evicted
	// from cache.
	cache := lru.New(capacity)

	ls := &LRUStore{
		dstore: dstore,
		lru:    cache,
	}

	// Load all keys from datastore into lru cache.
	if err := ls.loadKeys(ctx); err != nil {
		return nil, err
	}

	// Set the function to remove items from the datastore when they are
	// evicted from lru.
	cache.OnEvicted = func(key lru.Key, val interface{}) {
		// Remove item from datastore that was evicted from LRU.
		err := dstore.Delete(ls.rmCtx, key.(ds.Key))
		if err != nil {
			ls.rmErr = err
		}
	}

	return ls, nil
}

// Get implements datastore interface.
func (ls *LRUStore) Get(ctx context.Context, key ds.Key) ([]byte, error) {
	ls.lock.RLock()
	defer ls.lock.RUnlock()

	_, ok := ls.lru.Get(key)
	if !ok {
		return nil, ds.ErrNotFound
	}
	val, err := ls.dstore.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	return val, nil
}

// Put implements datastore interface.
func (ls *LRUStore) Put(ctx context.Context, key ds.Key, val []byte) error {
	ls.lock.Lock()
	defer ls.lock.Unlock()

	ls.rmCtx = ctx
	ls.lru.Add(key, nil)
	ls.rmCtx = nil

	// If error evicting old entries, remove key and return error.
	if ls.rmErr != nil {
		err := ls.rmErr
		ls.rmErr = nil
		ls.lru.Remove(key)
		return err
	}

	err := ls.dstore.Put(ctx, key, val)
	if err != nil {
		return err
	}
	return nil
}

// Delete implements datastore interface.
func (ls *LRUStore) Delete(ctx context.Context, key ds.Key) error {
	ls.lock.Lock()
	defer ls.lock.Unlock()

	ls.rmCtx = ctx
	ls.lru.Remove(key)
	ls.rmCtx = nil

	// If there was an error evicting old entries, return it.
	if ls.rmErr != nil {
		err := ls.rmErr
		ls.rmErr = nil
		return err
	}
	return nil
}

// GetSize implements datastore interface.
func (ls *LRUStore) GetSize(ctx context.Context, key ds.Key) (int, error) {
	ls.lock.RLock()
	defer ls.lock.RUnlock()

	return ls.dstore.GetSize(ctx, key)
}

// Has implements datastore interface.  Has determines if the item is present
// in the LRUStore without moving the item to the newest LRU position.
func (ls *LRUStore) Has(ctx context.Context, key ds.Key) (bool, error) {
	ls.lock.RLock()
	defer ls.lock.RUnlock()

	return ls.dstore.Has(ctx, key)
}

// Sync implements datastore interface.
func (ls *LRUStore) Sync(ctx context.Context, key ds.Key) error {
	ls.lock.Lock()
	defer ls.lock.Unlock()

	return ls.dstore.Sync(ctx, key)
}

// Close syncs the LRUStore entries but does not close the underlying
// datastore.  This is because LRUStore wraps an existing datastore and does
// not construct it, and the wrapped datastore may be in use elsewhere.
func (ls *LRUStore) Close() error {
	return ls.Sync(context.Background(), ds.NewKey(""))
}

// Query implements datastore interface.
func (ls *LRUStore) Query(ctx context.Context, q dsq.Query) (dsq.Results, error) {
	ls.lock.RLock()
	defer ls.lock.RUnlock()

	return ls.dstore.Query(ctx, q)
}

// Batch implements the Batching interface.
func (ls *LRUStore) Batch(ctx context.Context) (ds.Batch, error) {
	return ds.NewBasicBatch(ls), nil
}

// DiskUsage implements the PersistentDatastore interface.
func (ls *LRUStore) DiskUsage(ctx context.Context) (uint64, error) {
	return ds.DiskUsage(ctx, ls.dstore)
}

// Cap returns the LRUStore capacity.  Storing more than this number of items
// results in discarding oldest items.
func (ls *LRUStore) Cap() int {
	ls.lock.RLock()
	defer ls.lock.RUnlock()

	return ls.lru.MaxEntries
}

// Len returns the number of items in the LRUStore
func (ls *LRUStore) Len() int {
	ls.lock.RLock()
	defer ls.lock.RUnlock()

	return ls.lru.Len()
}

// Resize changes the LRUStore capacity. If the capacity is decreased below the
// number of items in the LRUStore, then oldest items are discarded until the
// LRUStore is filled to the new lower capacity.  Returns the number of items
// evicted from the LRUStore.
func (ls *LRUStore) Resize(ctx context.Context, newSize int) (int, error) {
	ls.lock.Lock()
	defer ls.lock.Unlock()

	diff := ls.lru.Len() - newSize
	if diff < 0 {
		diff = 0
	}
	if diff != 0 {
		ls.rmCtx = ctx
		for i := 0; i < diff; i++ {
			ls.lru.RemoveOldest()
			// If there was an error evicting old entries, return it.
			if ls.rmErr != nil {
				err := ls.rmErr
				ls.rmErr = nil
				ls.rmCtx = nil
				return 0, err
			}
		}
		ls.rmCtx = nil
	}

	ls.lru.MaxEntries = newSize
	return diff, nil
}

// Clear purges all stored items from the LRUStore.
func (ls *LRUStore) Clear(ctx context.Context) error {
	ls.lock.Lock()
	defer ls.lock.Unlock()

	ls.rmCtx = ctx
	ls.lru.Clear()
	ls.rmCtx = nil

	// If there was an error evicting old entries, return it.
	if ls.rmErr != nil {
		err := ls.rmErr
		ls.rmErr = nil
		return err
	}
	return nil
}

// loadKeys loads previously stored keys into LRU memory, without any LRU order.
func (ls *LRUStore) loadKeys(ctx context.Context) error {
	q := dsq.Query{
		KeysOnly: true,
	}

	results, err := ls.dstore.Query(ctx, q)
	if err != nil {
		return err
	}
	defer results.Close()

	origCap := ls.lru.MaxEntries
	ls.lru.MaxEntries = 0

	for r := range results.Next() {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if r.Error != nil {
			return fmt.Errorf("cannot read cache key: %s", r.Error)
		}

		ls.lru.Add(ds.RawKey(r.Entry.Key), nil)
	}

	// If the cache was resized to expand beyond its original capacity, then
	// set its size to only as big as the number of keys read from datastore.
	// This will be the number of links in the largest list.
	if ls.lru.Len() > origCap {
		ls.lru.MaxEntries = ls.lru.Len()
	} else {
		ls.lru.MaxEntries = origCap
	}
	return nil
}
