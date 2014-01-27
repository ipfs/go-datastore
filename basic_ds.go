package datastore

import (
  "log"
)

// Here are some basic datastore implementations.

// MapDatastore uses a standard Go map for internal storage.
type keyMap map[Key]interface{}
type MapDatastore struct {
  values keyMap
}

func NewMapDatastore() (d *MapDatastore) {
  return &MapDatastore{
    values: keyMap{},
  }
}

func (d *MapDatastore) Put(key Key, value interface{}) (err error) {
  d.values[key] = value
  return nil
}

func (d *MapDatastore) Get(key Key) (value interface{}, err error) {
  val, found := d.values[key]
  if !found {
    return nil, ErrNotFound
  }
  return val, nil
}

func (d *MapDatastore) Has(key Key) (exists bool, err error) {
  _, found := d.values[key]
  return found, nil
}

func (d *MapDatastore) Delete(key Key) (err error) {
  delete(d.values, key)
  return nil
}

// NullDatastore stores nothing, but conforms to the API.
// Useful to test with.
type NullDatastore struct {
}

func NewNullDatastore() (*NullDatastore) {
  return &NullDatastore{}
}

func (d *NullDatastore) Put(key Key, value interface{}) (err error) {
  return nil
}

func (d *NullDatastore) Get(key Key) (value interface{}, err error) {
  return nil, nil
}

func (d *NullDatastore) Has(key Key) (exists bool, err error) {
  return false, nil
}

func (d *NullDatastore) Delete(key Key) (err error) {
  return nil
}

// LogDatastore logs all accesses through the datastore.
type LogDatastore struct {
  Child Datastore
}

func NewLogDatastore(ds Datastore) (*LogDatastore) {
  return &LogDatastore{Child: ds}
}

func (d *LogDatastore) Put(key Key, value interface{}) (err error) {
  log.Printf("LogDatastore: Put %s", key)
  return d.Child.Put(key, value)
}

func (d *LogDatastore) Get(key Key) (value interface{}, err error) {
  log.Printf("LogDatastore: Get %s", key)
  return d.Child.Get(key)
}

func (d *LogDatastore) Has(key Key) (exists bool, err error) {
  log.Printf("LogDatastore: Has %s", key)
  return d.Child.Has(key)
}

func (d *LogDatastore) Delete(key Key) (err error) {
  log.Printf("LogDatastore: Delete %s", key)
  return d.Child.Delete(key)
}
