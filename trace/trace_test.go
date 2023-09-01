package trace

import (
	"testing"

	"github.com/ipfs/go-datastore"
	dstest "github.com/ipfs/go-datastore/test"
	"go.opentelemetry.io/otel"
)

func TestTraceAll(t *testing.T) {
	tracer := otel.Tracer("tracer")
	dstest.SubtestAll(t, New(datastore.NewMapDatastore(), tracer))
}
