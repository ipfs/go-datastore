package keytransform

import ds "github.com/ipfs/go-datastore"

// KeyMapping is a function that maps one key to annother
type KeyMapping func(ds.Key) ds.Key

// KeyTransform is an object with a pair of functions for (invertibly)
// transforming keys
type KeyTransform interface {
	ConvertKey(ds.Key) ds.Key
	InvertKey(ds.Key) ds.Key
}

// Pair is a convince struct for constructing a key transform.
type Pair struct {
	Convert KeyMapping
	Invert  KeyMapping
}

func (t *Pair) ConvertKey(k ds.Key) ds.Key {
	return t.Convert(k)
}

func (t *Pair) InvertKey(k ds.Key) ds.Key {
	return t.Invert(k)
}

var _ KeyTransform = (*Pair)(nil)
