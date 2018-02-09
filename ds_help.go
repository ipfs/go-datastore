package datastore

import (
	cid "github.com/ipfs/go-cid"
	"github.com/whyrusleeping/base32"
)

func NewKeyFromBinary(rawKey []byte) Key {
	buf := make([]byte, 1+base32.RawStdEncoding.EncodedLen(len(rawKey)))
	buf[0] = '/'
	base32.RawStdEncoding.Encode(buf[1:], rawKey)
	return RawKey(string(buf))
}

func BinaryFromDsKey(k Key) ([]byte, error) {
	return base32.RawStdEncoding.DecodeString(k.String()[1:])
}

func CidToDsKey(k *cid.Cid) Key {
	return NewKeyFromBinary(k.Bytes())
}

func DsKeyToCid(dsKey Key) (*cid.Cid, error) {
	kb, err := BinaryFromDsKey(dsKey)
	if err != nil {
		return nil, err
	}
	return cid.Cast(kb)
}
