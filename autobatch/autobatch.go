// Package autobatch provides a go-datastore implementation that
// automatically batches together writes by holding puts in memory until
// a certain threshold is met. It also acts as a debounce.
package autobatch

import (
	"sync"
	"time"

	ds "github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
	logging "github.com/ipfs/go-log"
)

var log = logging.Logger("datastore/autobatch")

// Datastore implements a go-datastore.
type Datastore struct {
	child ds.Batching

	mu     sync.RWMutex
	buffer map[ds.Key]op

	maxWrite int
	maxDelay time.Duration
	newWrite chan struct{}
	exit     chan struct{}
}

type op struct {
	delete bool
	value  []byte
}

// NewAutoBatching returns a new datastore that automatically
// batches writes using the given Batching datastore. The maximum number of
// write before triggering a batch is given by maxWrite. The maximum delay
// before triggering a batch is given by maxDelay.
func NewAutoBatching(child ds.Batching, maxWrite int, maxDelay time.Duration) *Datastore {
	d := &Datastore{
		child:    child,
		buffer:   make(map[ds.Key]op, maxWrite),
		maxWrite: maxWrite,
		maxDelay: maxDelay,
		newWrite: make(chan struct{}),
		exit:     make(chan struct{}),
	}
	go d.runBatcher()
	return d
}

func (d *Datastore) addOp(key ds.Key, op op) {
	d.mu.Lock()
	d.buffer[key] = op
	d.mu.Unlock()
	d.newWrite <- struct{}{}
}

func (d *Datastore) runBatcher() {
	var timer <-chan time.Time

	write := func() {
		timer = nil

		b, err := d.prepareBatch(nil)
		if err != nil {
			log.Error(err)
			return
		}
		err = b.Commit()
		if err != nil {
			log.Error(err)
			return
		}
	}

	for {
		select {
		case <-d.exit:
			return
		case <-timer:
			write()
		case <-d.newWrite:
			d.mu.RLock()
			ready := len(d.buffer)
			d.mu.RUnlock()
			if ready > d.maxWrite {
				write()
			}
			if timer == nil {
				timer = time.After(d.maxDelay)
			}
		}
	}
}

// Delete deletes a key/value
func (d *Datastore) Delete(k ds.Key) error {
	d.addOp(k, op{delete: true})
	return nil
}

// Get retrieves a value given a key.
func (d *Datastore) Get(k ds.Key) ([]byte, error) {
	d.mu.RLock()
	o, ok := d.buffer[k]
	d.mu.RUnlock()

	if ok {
		if o.delete {
			return nil, ds.ErrNotFound
		}
		return o.value, nil
	}

	return d.child.Get(k)
}

// Put stores a key/value.
func (d *Datastore) Put(k ds.Key, val []byte) error {
	d.addOp(k, op{value: val})
	return nil
}

// Sync flushes all operations on keys at or under the prefix
// from the current batch to the underlying datastore
func (d *Datastore) Sync(prefix ds.Key) error {
	b, err := d.prepareBatch(&prefix)
	if err != nil {
		return err
	}
	return b.Commit()
}

// Flush flushes the current batch to the underlying datastore.
func (d *Datastore) Flush() error {
	b, err := d.prepareBatch(nil)
	if err != nil {
		return err
	}
	return b.Commit()
}

func (d *Datastore) prepareBatch(prefix *ds.Key) (ds.Batch, error) {
	b, err := d.child.Batch()
	if err != nil {
		return nil, err
	}

	d.mu.Lock()

	for k, o := range d.buffer {
		if prefix != nil && !(k.Equal(*prefix) || k.IsDescendantOf(*prefix)) {
			continue
		}

		var err error
		if o.delete {
			err = b.Delete(k)
		} else {
			err = b.Put(k, o.value)
		}
		if err != nil {
			d.mu.Unlock()
			return nil, err
		}

		delete(d.buffer, k)
	}

	d.mu.Unlock()

	return b, nil
}

// Has checks if a key is stored.
func (d *Datastore) Has(k ds.Key) (bool, error) {
	d.mu.RLock()
	o, ok := d.buffer[k]
	d.mu.RUnlock()

	if ok {
		return !o.delete, nil
	}

	return d.child.Has(k)
}

// GetSize implements Datastore.GetSize
func (d *Datastore) GetSize(k ds.Key) (int, error) {
	d.mu.RLock()
	o, ok := d.buffer[k]
	d.mu.RUnlock()

	if ok {
		if o.delete {
			return -1, ds.ErrNotFound
		}
		return len(o.value), nil
	}

	return d.child.GetSize(k)
}

// Query performs a query
func (d *Datastore) Query(q dsq.Query) (dsq.Results, error) {
	err := d.Flush()
	if err != nil {
		return nil, err
	}

	return d.child.Query(q)
}

// DiskUsage implements the PersistentDatastore interface.
func (d *Datastore) DiskUsage() (uint64, error) {
	return ds.DiskUsage(d.child)
}

func (d *Datastore) Batch() (ds.Batch, error) {
	b, err := d.child.Batch()
	if err != nil {
		return nil, err
	}
	return &batch{
		parent:   d,
		child:    b,
		toDelete: make(map[ds.Key]struct{}),
	}, nil
}

func (d *Datastore) Close() error {
	err1 := d.Flush()
	err2 := d.child.Close()
	if err1 != nil {
		return err1
	}
	if err2 != nil {
		return err2
	}
	close(d.exit)
	close(d.newWrite)
	return nil
}

type batch struct {
	parent   *Datastore
	child    ds.Batch
	toDelete map[ds.Key]struct{}
}

func (b *batch) Put(key ds.Key, value []byte) error {
	delete(b.toDelete, key)
	return b.child.Put(key, value)
}

func (b *batch) Delete(key ds.Key) error {
	b.toDelete[key] = struct{}{}
	return b.child.Delete(key)
}

func (b *batch) Commit() error {
	b.parent.mu.Lock()
	for key := range b.toDelete {
		delete(b.parent.buffer, key)
	}
	b.parent.mu.Unlock()
	return b.child.Commit()
}
