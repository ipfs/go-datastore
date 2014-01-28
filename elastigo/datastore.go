package elastigo

import (
	"fmt"
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
}

func NewDatastore(addr Address, index string) (*Datastore, error) {
	if GlobalInstance.Host != "" && GlobalInstance != addr {
		return nil, fmt.Errorf("elastigo only allows one client. See godoc.")
	}

	api.Domain = addr.Host
	if addr.Port > 0 {
		api.Port = string(addr.Port)
	}

	GlobalInstance = addr
	return &Datastore{
		addr:  addr,
		index: index,
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
	res, err := core.Index(false, d.Index(key), key.Type(), key.Name(), value)
	if err != nil {
		return err
	}
	if !res.Ok {
		return fmt.Errorf("Elasticsearch response: NOT OK. %v", res)
	}
	return nil
}

func (d *Datastore) Get(key ds.Key) (value interface{}, err error) {
	res, err := core.Get(false, d.Index(key), key.Type(), key.Name())
	if err != nil {
		return nil, err
	}
	if !res.Ok {
		return nil, fmt.Errorf("Elasticsearch response: NOT OK. %v", res)
	}
	return res.Source, nil
}

func (d *Datastore) Has(key ds.Key) (exists bool, err error) {
	return core.Exists(false, d.Index(key), key.Type(), key.Name())
}

func (d *Datastore) Delete(key ds.Key) (err error) {
	res, err := core.Delete(false, d.Index(key), key.Type(), key.Name(), 0, "")
	if err != nil {
		return err
	}
	if !res.Ok {
		return fmt.Errorf("Elasticsearch response: NOT OK. %v", res)
	}
	return nil
}
