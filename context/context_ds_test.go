package contextds_test

import (
	"context"
	"testing"

	"github.com/ipfs/go-datastore"
	contextds "github.com/ipfs/go-datastore/context"
	"github.com/ipfs/go-datastore/query"
)

// AI generated tests and mocks

type mockWrite struct {
	putCalled    bool
	putKey       datastore.Key
	putValue     []byte
	putErr       error
	deleteCalled bool
	deleteKey    datastore.Key
	deleteErr    error
}

func (m *mockWrite) Put(ctx context.Context, key datastore.Key, value []byte) error {
	m.putCalled = true
	m.putKey = key
	m.putValue = value
	return m.putErr
}

func (m *mockWrite) Delete(ctx context.Context, key datastore.Key) error {
	m.deleteCalled = true
	m.deleteKey = key
	return m.deleteErr
}

type mockRead struct {
	getCalled bool
	getKey    datastore.Key
	getValue  []byte
	getErr    error
	hasCalled bool
	hasKey    datastore.Key
	hasValue  bool
	hasErr    error

	getSizeCalled bool
	getSizeKey    datastore.Key
	getSizeValue  int
	getSizeErr    error

	queryCalled  bool
	queryQ       query.Query
	queryResults query.Results
	queryErr     error
}

func (m *mockRead) Get(ctx context.Context, key datastore.Key) ([]byte, error) {
	m.getCalled = true
	m.getKey = key
	return m.getValue, m.getErr
}

func (m *mockRead) Has(ctx context.Context, key datastore.Key) (bool, error) {
	m.hasCalled = true
	m.hasKey = key
	return m.hasValue, m.hasErr
}

func (m *mockRead) GetSize(ctx context.Context, key datastore.Key) (int, error) {
	m.getSizeCalled = true
	m.getSizeKey = key
	return m.getSizeValue, m.getSizeErr
}

func (m *mockRead) Query(ctx context.Context, q query.Query) (query.Results, error) {
	m.queryCalled = true
	m.queryQ = q
	return m.queryResults, m.queryErr
}

func TestDatastore_WithWriteContext(t *testing.T) {
	inner := datastore.NewMapDatastore()
	mock := &mockWrite{}
	ds := contextds.WrapDatastore(inner)
	ctx := contextds.WithWrite(context.Background(), mock)

	key := datastore.NewKey("foo")
	value := []byte("bar")

	err := ds.Put(ctx, key, value)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	if !mock.putCalled || mock.putKey != key || string(mock.putValue) != string(value) {
		t.Errorf("Put did not delegate to mockWrite correctly")
	}

	err = ds.Delete(ctx, key)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	if !mock.deleteCalled || mock.deleteKey != key {
		t.Errorf("Delete did not delegate to mockWrite correctly")
	}
}

func TestDatastore_WithReadContext(t *testing.T) {
	inner := datastore.NewMapDatastore()
	mock := &mockRead{
		getValue:     []byte("baz"),
		hasValue:     true,
		getSizeValue: 3,
	}
	ds := contextds.WrapDatastore(inner)
	ctx := contextds.WithRead(context.Background(), mock)

	key := datastore.NewKey("foo")

	val, err := ds.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !mock.getCalled || mock.getKey != key || string(val) != "baz" {
		t.Errorf("Get did not delegate to mockRead correctly")
	}

	has, err := ds.Has(ctx, key)
	if err != nil {
		t.Fatalf("Has failed: %v", err)
	}
	if !mock.hasCalled || mock.hasKey != key || !has {
		t.Errorf("Has did not delegate to mockRead correctly")
	}

	sz, err := ds.GetSize(ctx, key)
	if err != nil {
		t.Fatalf("GetSize failed: %v", err)
	}
	if !mock.getSizeCalled || mock.getSizeKey != key || sz != 3 {
		t.Errorf("GetSize did not delegate to mockRead correctly")
	}

	q := query.Query{}
	_, err = ds.Query(ctx, q)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if !mock.queryCalled {
		t.Errorf("Query did not delegate to mockRead correctly")
	}
}
