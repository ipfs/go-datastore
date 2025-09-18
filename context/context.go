package contextds

import (
	"context"

	"github.com/ipfs/go-datastore"
)

// WithWrite adds a write batch to the context.
func WithWrite(ctx context.Context, batch datastore.Write) context.Context {
	return context.WithValue(ctx, writeKey, batch)
}

// GetWrite retrieves the write batch from the context.
func GetWrite(ctx context.Context) (datastore.Write, bool) {
	batch, ok := ctx.Value(writeKey).(datastore.Write)
	return batch, ok
}

// WithRead adds a read batch to the context.
func WithRead(ctx context.Context, batch datastore.Read) context.Context {
	return context.WithValue(ctx, readKey, batch)
}

// GetRead retrieves the read batch from the context.
func GetRead(ctx context.Context) (datastore.Read, bool) {
	batch, ok := ctx.Value(readKey).(datastore.Read)
	return batch, ok
}

type (
	writeKeyTp int
	readKeyTp  int
)

var (
	writeKey writeKeyTp
	readKey  readKeyTp
)
