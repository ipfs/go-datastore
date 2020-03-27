package fuzzer

import (
	"context"
	"io"
	"sync/atomic"

	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-ds-badger"
)

// DsOpener is the concrete datastore. that Fuzz will fuzz against.
var DsOpener func() ds.TxnDatastore
var dsInst ds.TxnDatastore

// Threads is a measure of concurrency.
var Threads int

func init() {
	DsOpener = func() ds.TxnDatastore {
		d, _ := badger.NewDatastore("tmp", &badger.DefaultOptions)
		return d
	}
	Threads = 1

	keyCache[0] = ds.NewKey("/")
	cachedKeys = 1
}

func setup() ([]chan byte, context.CancelFunc) {
	// TODO: dynamic thread starting.
	ctx, cncl := context.WithCancel(context.Background())

	if dsInst != nil {
		dsInst.Close()
	}
	dsInst = DsOpener()

	drivers := make([]chan byte, Threads)
	for i := 0; i < Threads; i++ {
		drivers[i] = make(chan byte, 15)
		go threadDriver(ctx, drivers[i])
	}
	return drivers, cncl
}

// Fuzz is a go-fuzzer compatible input point for replaying
// data (interpreted as a script of commands)
// to a chosen ipfs datastore implementation
func Fuzz(data []byte) int {
	drivers, cncl := setup()
	drive(drivers, data)
	for i := 0; i < Threads; i++ {
		close(drivers[i])
	}
	cncl()
	dsInst.Close()
	return 0
}

func drive(drivers []chan byte, data []byte) {
	for i, b := range data {
		drivers[i%Threads] <- b
	}
}

// FuzzStream does the same as fuzz but with streaming input
func FuzzStream(data io.Reader) int {
	drivers, cncl := setup()
	b := make([]byte, 4096)
	for {
		n, _ := data.Read(b)
		if n == 0 {
			break
		}
		drive(drivers, b[:n])
	}
	for i := 0; i < Threads; i++ {
		close(drivers[i])
	}
	cncl()
	dsInst.Close()
	return 0
}

type op byte

const (
	opNone op = iota
	opGet
	opHas
	opGetSize
	opQuery
	opPut
	opDelete
	opNewTX
	opCommitTX
	opDiscardTX
	opMax
)

type state struct {
	op
	keyReady bool
	key      ds.Key
	valReady bool
	val      []byte
	reader   ds.Read
	writer   ds.Write
	txn      ds.Txn
}

func threadDriver(ctx context.Context, cmnds chan byte) error {
	s := state{}
	s.reader = dsInst
	s.writer = dsInst

	for {
		select {
		case c, ok := <-cmnds:
			if !ok {
				return nil
			}
			_ = nextState(&s, c)
		case <-ctx.Done():
			return nil
		}
	}
}

func nextState(s *state, c byte) error {
	if s.op == opNone {
		s.op = op(c) % opMax
		return nil
	} else if s.op == opGet {
		if !s.keyReady {
			return makeKey(s, c)
		}
		s.reader.Get(s.key)
		reset(s)
		return nil
	} else if s.op == opHas {
		if !s.keyReady {
			return makeKey(s, c)
		}
		s.reader.Has(s.key)
		reset(s)
		return nil
	} else if s.op == opGetSize {
		if !s.keyReady {
			return makeKey(s, c)
		}
		s.reader.GetSize(s.key)
		reset(s)
		return nil
	} else if s.op == opQuery {
		// TODO
	} else if s.op == opPut {
		if !s.keyReady {
			return makeKey(s, c)
		}
		if !s.valReady {
			return makeValue(s, c)
		}
		s.writer.Put(s.key, s.val)
		reset(s)
		return nil
	} else if s.op == opDelete {
		if !s.keyReady {
			return makeKey(s, c)
		}
		s.writer.Delete(s.key)
		reset(s)
		return nil
	} else if s.op == opNewTX {
		if s.txn == nil {
			s.txn, _ = dsInst.NewTransaction(((c & 1) == 1))
			if (c & 1) != 1 { // read+write
				s.writer = s.txn
			}
			s.reader = s.txn
		}
		reset(s)
		return nil
	} else if s.op == opCommitTX {
		if s.txn != nil {
			s.txn.Discard()
			s.txn = nil
			s.reader = dsInst
			s.writer = dsInst
		}
		reset(s)
		return nil
	} else if s.op == opDiscardTX {
		if s.txn != nil {
			s.txn.Discard()
			s.txn = nil
			s.reader = dsInst
			s.writer = dsInst
		}
		reset(s)
		return nil
	}
	return nil
}

func reset(s *state) {
	s.op = opNone
	s.keyReady = false
	s.key = ds.RawKey("")
	s.valReady = false
}

var keyCache [128]ds.Key
var cachedKeys int32

func makeKey(s *state, c byte) error {
	keys := atomic.LoadInt32(&cachedKeys)
	if keys > 128 {
		keys = 128
	}
	if c&1 == 1 {
		// 50% chance we want to-reuse an existing key
		s.key = keyCache[(c>>1)%byte(keys)]
		s.keyReady = true
	} else {
		s.key = ds.RandomKey()
		// half the time we'll make it a child of an existing key
		if c&2 == 2 {
			s.key = keyCache[(c>>1)%byte(keys)].Child(s.key)
		}
		// new key
		if keys < 128 {
			keys = atomic.AddInt32(&cachedKeys, 1)
			if keys >= 128 {
				atomic.StoreInt32(&cachedKeys, 128)
			} else {
				keyCache[keys-1] = s.key
			}
		}
		s.keyReady = true
	}
	return nil
}

func makeValue(s *state, c byte) error {
	s.val = make([]byte, c)
	s.val[0] = 1
	s.valReady = true
	return nil
}
