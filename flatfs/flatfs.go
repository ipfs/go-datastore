// Package flatfs is a Datastore implementation that stores all
// objects in a two-level directory structure in the local file
// system, regardless of the hierarchy of the keys.
package flatfs

import (
	"encoding/hex"
	"errors"
	"io/ioutil"
	"os"
	"path"
	"strings"

	"github.com/jbenet/go-datastore"
	"github.com/jbenet/go-datastore/query"
)

const (
	extension    = ".data"
	maxPrefixLen = 16
)

var (
	ErrBadPrefixLen = errors.New("bad prefix length")
)

type Datastore struct {
	path string
	// length of the dir splay prefix, in bytes of hex digits
	hexPrefixLen int
}

var _ datastore.Datastore = (*Datastore)(nil)

func New(path string, prefixLen int) (*Datastore, error) {
	if prefixLen <= 0 || prefixLen > maxPrefixLen {
		return nil, ErrBadPrefixLen
	}
	fs := &Datastore{
		path: path,
		// convert from binary bytes to bytes of hex encoding
		hexPrefixLen: prefixLen * hex.EncodedLen(1),
	}
	return fs, nil
}

var padding = strings.Repeat("_", maxPrefixLen*hex.EncodedLen(1))

func (fs *Datastore) encode(key datastore.Key) (dir, file string) {
	safe := hex.EncodeToString(key.Bytes())
	prefix := (safe + padding)[:fs.hexPrefixLen]
	dir = path.Join(fs.path, prefix)
	file = path.Join(dir, safe+extension)
	return dir, file
}

func (fs *Datastore) Put(key datastore.Key, value interface{}) error {
	val, ok := value.([]byte)
	if !ok {
		return datastore.ErrInvalidType
	}

	dir, path := fs.encode(key)
	if err := os.Mkdir(dir, 0777); err != nil {
		// EEXIST is safe to ignore here, that just means the prefix
		// directory already existed.
		if !os.IsExist(err) {
			return err
		}
	}

	tmp, err := ioutil.TempFile(dir, "put-")
	if err != nil {
		return err
	}
	closed := false
	removed := false
	defer func() {
		if !closed {
			// silence errcheck
			_ = tmp.Close()
		}
		if !removed {
			// silence errcheck
			_ = os.Remove(tmp.Name())
		}
	}()

	if _, err := tmp.Write(val); err != nil {
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	closed = true

	err = os.Rename(tmp.Name(), path)
	if err != nil {
		return err
	}
	removed = true

	return nil
}

func (fs *Datastore) Get(key datastore.Key) (value interface{}, err error) {
	_, path := fs.encode(key)
	data, err := ioutil.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, datastore.ErrNotFound
		}
		// no specific error to return, so just pass it through
		return nil, err
	}
	return data, nil
}

func (fs *Datastore) Has(key datastore.Key) (exists bool, err error) {
	_, path := fs.encode(key)
	switch _, err := os.Stat(path); {
	case err == nil:
		return true, nil
	case os.IsNotExist(err):
		return false, nil
	default:
		return false, err
	}
}

func (fs *Datastore) Delete(key datastore.Key) error {
	return errors.New("TODO")
}

func (fs *Datastore) Query(q query.Query) (query.Results, error) {
	return nil, errors.New("TODO")
}

var _ datastore.ThreadSafeDatastore = (*Datastore)(nil)

func (*Datastore) IsThreadSafe() {}
