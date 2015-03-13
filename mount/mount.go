// Package mount provides a Datastore that has other Datastores
// mounted at various key prefixes.
package mount

import (
	"errors"
	"strings"

	"github.com/jbenet/go-datastore"
	"github.com/jbenet/go-datastore/query"
)

var (
	ErrNoMount = errors.New("no datastore mounted for this key")
)

type Mount struct {
	Prefix    datastore.Key
	Datastore datastore.Datastore
}

func New(mounts []Mount) *Datastore {
	// make a copy so we're sure it doesn't mutate
	m := make([]Mount, len(mounts))
	for i, v := range mounts {
		m[i] = v
	}
	return &Datastore{mounts: m}
}

type Datastore struct {
	mounts []Mount
}

var _ datastore.Datastore = (*Datastore)(nil)

func (d *Datastore) lookup(key datastore.Key) (datastore.Datastore, datastore.Key) {
	for _, m := range d.mounts {
		if m.Prefix.IsAncestorOf(key) {
			s := strings.TrimPrefix(key.String(), m.Prefix.String())
			k := datastore.NewKey(s)
			return m.Datastore, k
		}
	}
	return nil, key
}

func (d *Datastore) Put(key datastore.Key, value interface{}) error {
	ds, k := d.lookup(key)
	if ds == nil {
		return ErrNoMount
	}
	return ds.Put(k, value)
}

func (d *Datastore) Get(key datastore.Key) (value interface{}, err error) {
	ds, k := d.lookup(key)
	if ds == nil {
		return nil, datastore.ErrNotFound
	}
	return ds.Get(k)
}

func (d *Datastore) Has(key datastore.Key) (exists bool, err error) {
	ds, k := d.lookup(key)
	if ds == nil {
		return false, nil
	}
	return ds.Has(k)
}

func (d *Datastore) Delete(key datastore.Key) error {
	return errors.New("TODO")
}

func (d *Datastore) Query(q query.Query) (query.Results, error) {
	return nil, errors.New("TODO")
}
