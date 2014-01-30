package elastigo

import (
	"fmt"
	"github.com/codahale/blake2"
	ds "github.com/jbenet/datastore.go"
	"github.com/mattbaird/elastigo/api"
	"github.com/mattbaird/elastigo/core"
)

// Currently, elastigo does not allow connecting to multiple elasticsearch
// instances. The elastigo API uses global static variables (ugh).
// See https://github.com/mattbaird/elastigo/issues/22
//
// Thus, we use a global static variable (GlobalInstance), and return an
// error if NewDatastore is called twice with different addresses.
var GlobalInstance Address

type Address struct {
	Host string
	Port int
}

// Datastore uses a standard Go map for internal storage.
type Datastore struct {
	addr  Address
	index string

	// Elastic search does not allow slashes in their object ids,
	// so we hash the key. By default, we use the provided BlakeKeyHash
	KeyHash func(ds.Key) string
}

func NewDatastore(addr Address, index string) (*Datastore, error) {
	if GlobalInstance.Host != "" && GlobalInstance != addr {
		return nil, fmt.Errorf("elastigo only allows one client. See godoc.")
	}

	api.Domain = addr.Host
	if addr.Port > 0 {
		api.Port = fmt.Sprintf("%d", addr.Port)
	}

	GlobalInstance = addr
	return &Datastore{
		addr:    addr,
		index:   index,
		KeyHash: BlakeKeyHash,
	}, nil
}

// Returns the ElasticSearch index for given key. If the datastore specifies
// an index, use that. Else, key.Parent
func (d *Datastore) Index(key ds.Key) string {
	if len(d.index) > 0 {
		return d.index
	}
	return key.Parent().BaseNamespace()
}

// value should be JSON serializable.
func (d *Datastore) Put(key ds.Key, value interface{}) (err error) {
	id := d.KeyHash(key)
	res, err := core.Index(false, d.Index(key), key.Type(), id, value)
	if err != nil {
		return err
	}
	if !res.Ok {
		return fmt.Errorf("Elasticsearch response: NOT OK. %v", res)
	}
	return nil
}

func (d *Datastore) Get(key ds.Key) (value interface{}, err error) {
	id := d.KeyHash(key)
	res, err := core.Get(false, d.Index(key), key.Type(), id)
	if err != nil {
		return nil, err
	}
	if !res.Ok {
		return nil, fmt.Errorf("Elasticsearch response: NOT OK. %v", res)
	}
	return res.Source, nil
}

func (d *Datastore) Has(key ds.Key) (exists bool, err error) {
	id := d.KeyHash(key)
	return core.Exists(false, d.Index(key), key.Type(), id)
}

func (d *Datastore) Delete(key ds.Key) (err error) {
	id := d.KeyHash(key)
	res, err := core.Delete(false, d.Index(key), key.Type(), id, 0, "")
	if err != nil {
		return err
	}
	if !res.Ok {
		return fmt.Errorf("Elasticsearch response: NOT OK. %v", res)
	}
	return nil
}

// Hash a key and return the first 16 hex chars of its blake2b hash.
// basically: Blake2b(key).HexString[:16]
func BlakeKeyHash(key ds.Key) string {
	h := blake2.NewBlake2B()
	h.Write(key.Bytes())
	d := h.Sum(nil)
	return fmt.Sprintf("%x", d)[:16]
}
