package fuzzer

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sync"
	"sync/atomic"

	ds "github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
)

//go:generate go run ./cmd/generate

// openers contains the known datastore implementations.
var openers map[string]func(string) ds.Datastore

// AddOpener allows registration of a new driver for fuzzing.
func AddOpener(name string, opener func(loc string) ds.Datastore) {
	if openers == nil {
		openers = make(map[string]func(string) ds.Datastore)
	}
	openers[name] = opener
}

// Threads is a measure of concurrency.
// Note: if Threads > 1, determinism is not guaranteed.
var Threads int

func init() {
	if openers == nil {
		openers = make(map[string]func(string) ds.Datastore)
	}
	Threads = 1
}

// RunState encapulates the state of a given fuzzing run
type RunState struct {
	inst ds.Datastore
	// opMax signals which operations the instance supports.
	opMax         op
	inputChannels []chan<- byte
	wg            sync.WaitGroup
	Cancel        context.CancelFunc

	keyCache   [128]ds.Key
	cachedKeys int32
	ctr        int32 //nolint:structcheck,unused
}

// DB returns the datastore being driven by this instance
func (r *RunState) DB() ds.Datastore {
	return r.inst
}

// TxnDB returns the transaciton database if the store under test supports transactions
func (r *RunState) TxnDB() ds.TxnDatastore {
	if txdb, ok := r.inst.(ds.TxnDatastore); ok {
		return txdb
	}
	return nil
}

type threadState struct {
	op
	keyReady bool
	key      ds.Key
	valReady bool
	val      []byte
	reader   ds.Read
	writer   ds.Write
	txn      ds.Txn
	*RunState
}

// Open instantiates an instance of the database implementation for testing.
func Open(driver string, location string, cleanup bool) (*RunState, error) {
	ctx, cncl := context.WithCancel(context.Background())

	opener, ok := openers[driver]
	if !ok {
		cncl()
		return nil, fmt.Errorf("no such driver: %s", driver)
	}

	state := RunState{}
	state.inst = opener(location)
	state.opMax = opMax
	// don't attempt transaction operations on non-txn datastores.
	if state.TxnDB() == nil {
		state.opMax = opNewTX
	}
	state.keyCache[0] = ds.NewKey("/")
	state.cachedKeys = 1

	state.wg.Add(Threads)

	// wrap the context cancel to block until everythign is fully closed.
	doneCh := make(chan struct{})
	state.Cancel = func() {
		for i := 0; i < Threads; i++ {
			close(state.inputChannels[i])
		}
		cncl()
		<-doneCh
	}
	go func() {
		state.wg.Wait()
		state.inst.Close()
		if cleanup {
			os.RemoveAll(location)
		}
		close(doneCh)
	}()

	state.inputChannels = make([]chan<- byte, Threads)
	for i := 0; i < Threads; i++ {
		dr := make(chan byte, 15)
		go threadDriver(ctx, &state, dr)
		state.inputChannels[i] = dr
	}
	return &state, nil
}

// Fuzz is a go-fuzzer compatible input point for replaying
// data (interpreted as a script of commands)
// to known ipfs datastore implementations
func Fuzz(data []byte) int {
	var impls []string
	for impl := range openers {
		impls = append(impls, impl)
	}

	defaultLoc, _ := ioutil.TempDir("", "fuzz-*")

	if len(impls) == 0 {
		fmt.Fprintf(os.Stderr, "No datastores to fuzz.\n")
		return -1
	} else if len(impls) == 1 {
		return FuzzDB(impls[0], defaultLoc, true, data)
	} else {
		impl := impls[int(data[0])%len(impls)]
		return FuzzDB(impl, defaultLoc, true, data[1:])
	}
}

// FuzzDB fuzzes a given database entry, providing sufficient hooks to be
// used by CLI commands.
func FuzzDB(driver string, location string, cleanup bool, data []byte) int {
	inst, err := Open(driver, location, cleanup)
	if err != nil {
		return -1
	}
	inst.Fuzz(data)
	inst.Cancel()
	return 0
}

// FuzzStream does the same as fuzz but with streaming input
func FuzzStream(driver string, location string, cleanup bool, data io.Reader) error {
	inst, err := Open(driver, location, cleanup)
	if err != nil {
		return err
	}

	inst.FuzzStream(data)
	inst.Cancel()
	return nil
}

// Fuzz sends a set of bytes to drive the current open datastore instance.
func (r *RunState) Fuzz(data []byte) {
	for i, b := range data {
		r.inputChannels[i%Threads] <- b
	}
}

// FuzzStream sends a set of bytes to drive the current instance from a reader.
func (r *RunState) FuzzStream(data io.Reader) {
	b := make([]byte, 4096)
	for {
		n, _ := data.Read(b)
		if n == 0 {
			break
		}
		r.Fuzz(b[:n])
	}
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
	opSync
	opNewTX
	opCommitTX
	opDiscardTX
	opMax
)

func threadDriver(ctx context.Context, runState *RunState, cmnds chan byte) {
	defer runState.wg.Done()
	s := threadState{}
	s.RunState = runState
	s.reader = runState.inst
	s.writer = runState.inst

	for {
		select {
		case c, ok := <-cmnds:
			if !ok {
				return
			}
			_ = nextState(ctx, &s, c)
		case <-ctx.Done():
			return
		}
	}
}

func nextState(ctx context.Context, s *threadState, c byte) error {
	if s.op == opNone {
		s.op = op(c) % s.RunState.opMax
		return nil
	} else if s.op == opGet {
		if !s.keyReady {
			return makeKey(s, c)
		}
		_, _ = s.reader.Get(ctx, s.key)
		reset(s)
		return nil
	} else if s.op == opHas {
		if !s.keyReady {
			return makeKey(s, c)
		}
		_, _ = s.reader.Has(ctx, s.key)
		reset(s)
		return nil
	} else if s.op == opGetSize {
		if !s.keyReady {
			return makeKey(s, c)
		}
		_, _ = s.reader.GetSize(ctx, s.key)
		reset(s)
		return nil
	} else if s.op == opQuery {
		r, _ := s.reader.Query(ctx, dsq.Query{})
		defer r.Close()
		reset(s)

		for e := range r.Next() {
			if e.Error != nil {
				return nil
			}
		}
		return nil
	} else if s.op == opPut {
		if !s.keyReady {
			return makeKey(s, c)
		}
		if !s.valReady {
			return makeValue(s, c)
		}
		_ = s.writer.Put(ctx, s.key, s.val)
		reset(s)
		return nil
	} else if s.op == opDelete {
		if !s.keyReady {
			return makeKey(s, c)
		}
		_ = s.writer.Delete(ctx, s.key)
		reset(s)
		return nil
	} else if s.op == opNewTX {
		if s.txn == nil {
			if tdb := s.RunState.TxnDB(); tdb != nil {
				s.txn, _ = tdb.NewTransaction(((c & 1) == 1))
				if (c & 1) != 1 { // read+write
					s.writer = s.txn
				}
				s.reader = s.txn
			}
		}
		reset(s)
		return nil
	} else if s.op == opCommitTX {
		if s.txn != nil {
			s.txn.Discard()
			s.txn = nil
			s.reader = s.RunState.inst
			s.writer = s.RunState.inst
		}
		reset(s)
		return nil
	} else if s.op == opDiscardTX {
		if s.txn != nil {
			s.txn.Discard()
			s.txn = nil
			s.reader = s.RunState.inst
			s.writer = s.RunState.inst
		}
		reset(s)
		return nil
	} else if s.op == opSync {
		if !s.keyReady {
			return makeKey(s, c)
		}
		_ = s.RunState.inst.Sync(ctx, s.key)
		reset(s)
		return nil
	}
	return nil
}

func reset(s *threadState) {
	s.op = opNone
	s.keyReady = false
	s.key = ds.RawKey("")
	s.valReady = false
}

func makeKey(s *threadState, c byte) error {
	keys := atomic.LoadInt32(&s.RunState.cachedKeys)
	if keys > 128 {
		keys = 128
	}
	if c&1 == 1 {
		// 50% chance we want to-reuse an existing key
		s.key = s.RunState.keyCache[(c>>1)%byte(keys)]
		s.keyReady = true
	} else {
		s.key = ds.NewKey(fmt.Sprintf("key-%d", atomic.AddInt32(&s.ctr, 1)))
		// half the time we'll make it a child of an existing key
		if c&2 == 2 {
			s.key = s.RunState.keyCache[(c>>1)%byte(keys)].Child(s.key)
		}
		// new key
		if keys < 128 {
			keys = atomic.AddInt32(&s.RunState.cachedKeys, 1)
			if keys >= 128 {
				atomic.StoreInt32(&s.RunState.cachedKeys, 128)
			} else {
				s.RunState.keyCache[keys-1] = s.key
			}
		}
		s.keyReady = true
	}
	return nil
}

func makeValue(s *threadState, c byte) error {
	s.val = make([]byte, c)
	if c != 0 {
		s.val[0] = 1
	}
	s.valReady = true
	return nil
}
