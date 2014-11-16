package datastore

import (
	"errors"
	"io"
)

/*
Datastore represents storage for any key-value pair.

Datastores are general enough to be backed by all kinds of different storage:
in-memory caches, databases, a remote datastore, flat files on disk, etc.

The general idea is to wrap a more complicated storage facility in a simple,
uniform interface, keeping the freedom of using the right tools for the job.
In particular, a Datastore can aggregate other datastores in interesting ways,
like sharded (to distribute load) or tiered access (caches before databases).

While Datastores should be written general enough to accept all sorts of
values, some implementations will undoubtedly have to be specific (e.g. SQL
databases where fields should be decomposed into columns), particularly to
support queries efficiently. Moreover, certain datastores may enforce certain
types of values (e.g. requiring an io.Reader, a specific struct, etc) or
serialization formats (JSON, Protobufs, etc).

IMPORTANT: No Datastore should ever Panic! This is a cross-module interface,
and thus it should behave predictably and handle exceptional conditions with
proper error reporting. Thus, all Datastore calls may return errors, which
should be checked by callers.
*/
type Datastore interface {
	// Put stores the object `value` named by `key`.
	//
	// The generalized Datastore interface does not impose a value type,
	// allowing various datastore middleware implementations (which do not
	// handle the values directly) to be composed together.
	//
	// Ultimately, the lowest-level datastore will need to do some value checking
	// or risk getting incorrect values. It may also be useful to expose a more
	// type-safe interface to your application, and do the checking up-front.
	Put(key Key, value interface{}) (err error)

	// Get retrieves the object `value` named by `key`.
	// Get will return ErrNotFound if the key is not mapped to a value.
	Get(key Key) (value interface{}, err error)

	// Has returns whether the `key` is mapped to a `value`.
	// In some contexts, it may be much cheaper only to check for existence of
	// a value, rather than retrieving the value itself. (e.g. HTTP HEAD).
	// The default implementation is found in `GetBackedHas`.
	Has(key Key) (exists bool, err error)

	// Delete removes the value for given `key`.
	Delete(key Key) (err error)

	// KeyList returns a list of keys in the datastore
	KeyList() ([]Key, error)
}

// ThreadSafeDatastore is an interface that all threadsafe datastore should
// implement to leverage type safety checks.
type ThreadSafeDatastore interface {
	Datastore
	IsThreadSafe()
}

// ThreadSafeDatastoreCloser extends the ThreadSafeDatastore with the io.Closer
// interface
type ThreadSafeDatastoreCloser interface {
	ThreadSafeDatastore
	io.Closer
}

// Errors

// ErrNotFound is returned by Get, Has, and Delete when a datastore does not
// map the given key to a value.
var ErrNotFound = errors.New("datastore: key not found")

// ErrInvalidType is returned by Put when a given value is incopatible with
// the type the datastore supports. This means a conversion (or serialization)
// is needed beforehand.
var ErrInvalidType = errors.New("datastore: invalid type error")

// GetBackedHas provides a default Datastore.Has implementation.
// It exists so Datastore.Has implementations can use it, like so:
//
// func (*d SomeDatastore) Has(key Key) (exists bool, err error) {
//   return GetBackedHas(d, key)
// }
func GetBackedHas(ds Datastore, key Key) (bool, error) {
	_, err := ds.Get(key)
	switch err {
	case nil:
		return true, nil
	case ErrNotFound:
		return false, nil
	default:
		return false, err
	}
}
