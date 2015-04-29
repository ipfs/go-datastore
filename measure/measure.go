// Package measure provides a Datastore wrapper that records metrics
// using github.com/codahale/metrics.
package measure

import (
	"time"

	"github.com/jbenet/go-datastore"
	"github.com/jbenet/go-datastore/Godeps/_workspace/src/github.com/codahale/metrics"
	"github.com/jbenet/go-datastore/query"
)

// Histogram measurements exceeding these limits are dropped. TODO
// maybe it would be better to cap the value? Should we keep track of
// drops?
const (
	maxLatency = int64(1 * time.Second)
	maxSize    = int64(1 << 32)
)

// New wraps the datastore, providing metrics on the operations. The
// metrics are registered with names starting with prefix and a dot.
func New(prefix string, ds datastore.Datastore) datastore.Datastore {
	m := &measure{
		backend: ds,

		putNum:     metrics.Counter(prefix + ".Put.num"),
		putErr:     metrics.Counter(prefix + ".Put.err"),
		putLatency: metrics.NewHistogram(prefix+".Put.latency", 0, maxLatency, 3),
		putSize:    metrics.NewHistogram(prefix+".Put.size", 0, maxSize, 3),

		getNum:     metrics.Counter(prefix + ".Get.num"),
		getErr:     metrics.Counter(prefix + ".Get.err"),
		getLatency: metrics.NewHistogram(prefix+".Get.latency", 0, maxLatency, 3),
		getSize:    metrics.NewHistogram(prefix+".Get.size", 0, maxSize, 3),

		hasNum:     metrics.Counter(prefix + ".Has.num"),
		hasErr:     metrics.Counter(prefix + ".Has.err"),
		hasLatency: metrics.NewHistogram(prefix+".Has.latency", 0, maxLatency, 3),

		deleteNum:     metrics.Counter(prefix + ".Delete.num"),
		deleteErr:     metrics.Counter(prefix + ".Delete.err"),
		deleteLatency: metrics.NewHistogram(prefix+".Delete.latency", 0, maxLatency, 3),

		queryNum:     metrics.Counter(prefix + ".Query.num"),
		queryErr:     metrics.Counter(prefix + ".Query.err"),
		queryLatency: metrics.NewHistogram(prefix+".Query.latency", 0, maxLatency, 3),
	}
	return m
}

type measure struct {
	backend datastore.Datastore

	putNum     metrics.Counter
	putErr     metrics.Counter
	putLatency *metrics.Histogram
	putSize    *metrics.Histogram

	getNum     metrics.Counter
	getErr     metrics.Counter
	getLatency *metrics.Histogram
	getSize    *metrics.Histogram

	hasNum     metrics.Counter
	hasErr     metrics.Counter
	hasLatency *metrics.Histogram

	deleteNum     metrics.Counter
	deleteErr     metrics.Counter
	deleteLatency *metrics.Histogram

	queryNum     metrics.Counter
	queryErr     metrics.Counter
	queryLatency *metrics.Histogram
}

var _ datastore.Datastore = (*measure)(nil)

func recordLatency(h *metrics.Histogram, start time.Time) {
	elapsed := time.Now().Sub(start) / time.Microsecond
	_ = h.RecordValue(int64(elapsed))
}

func (m *measure) Put(key datastore.Key, value interface{}) error {
	defer recordLatency(m.putLatency, time.Now())
	m.putNum.Add()
	if b, ok := value.([]byte); ok {
		_ = m.putSize.RecordValue(int64(len(b)))
	}
	err := m.backend.Put(key, value)
	if err != nil {
		m.putErr.Add()
	}
	return err
}

func (m *measure) Get(key datastore.Key) (value interface{}, err error) {
	defer recordLatency(m.getLatency, time.Now())
	m.getNum.Add()
	value, err = m.backend.Get(key)
	if err != nil {
		m.getErr.Add()
	} else {
		if b, ok := value.([]byte); ok {
			_ = m.getSize.RecordValue(int64(len(b)))
		}
	}
	return value, err
}

func (m *measure) Has(key datastore.Key) (exists bool, err error) {
	defer recordLatency(m.hasLatency, time.Now())
	m.hasNum.Add()
	exists, err = m.backend.Has(key)
	if err != nil {
		m.hasErr.Add()
	}
	return exists, err
}

func (m *measure) Delete(key datastore.Key) error {
	defer recordLatency(m.deleteLatency, time.Now())
	m.deleteNum.Add()
	err := m.backend.Delete(key)
	if err != nil {
		m.deleteErr.Add()
	}
	return err
}

func (m *measure) Query(q query.Query) (query.Results, error) {
	defer recordLatency(m.queryLatency, time.Now())
	m.queryNum.Add()
	res, err := m.backend.Query(q)
	if err != nil {
		m.queryErr.Add()
	}
	return res, err
}
