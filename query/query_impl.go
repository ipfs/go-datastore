package query

func DerivedResults(qr Results, ch <-chan Result) Results {
	return &results{
		query: qr.Query(),
		proc:  qr.Process(),
		res:   ch,
	}
}

// NaiveFilter applies a filter to the results.
func NaiveFilter(qr Results, filter Filter) Results {
	ch := make(chan Result)
	go func() {
		defer close(ch)

		for e := range qr.Next() {
			if e.Error != nil || filter.Filter(e.Entry) {
				ch <- e
			}
		}
	}()

	return DerivedResults(qr, ch)
}

// NaiveLimit truncates the results to a given int limit
func NaiveLimit(qr Results, limit int) Results {
	ch := make(chan Result)
	go func() {
		defer close(ch)

		l := 0
		for e := range qr.Next() {
			if e.Error != nil || (l < limit) {
				ch <- e
			}
			l++
		}
	}()

	return DerivedResults(qr, ch)
}

// NaiveOffset skips a given number of results
func NaiveOffset(qr Results, offset int) Results {
	ch := make(chan Result)
	go func() {
		defer close(ch)

		sent := 0
		for e := range qr.Next() {
			if e.Error != nil {
				ch <- e
			}

			if sent >= offset {
				ch <- e
				sent++
			}
		}
	}()

	return DerivedResults(qr, ch)
}

// NaiveOrder reorders results according to given Order.
// WARNING: this is the only non-stream friendly operation!
func NaiveOrder(qr Results, o Order) Results {
	ch := make(chan Result)
	var entries []Entry
	go func() {
		defer close(ch)

		for e := range qr.Next() {
			if e.Error != nil {
				ch <- e
			}

			entries = append(entries, e.Entry)
		}

		o.Sort(entries)
		for _, e := range entries {
			ch <- Result{Entry: e}
		}
	}()

	return DerivedResults(qr, ch)
}

func NaiveQueryApply(q Query, qr Results) Results {
	if q.Prefix != "" {
		qr = NaiveFilter(qr, FilterKeyPrefix{q.Prefix})
	}
	for _, f := range q.Filters {
		qr = NaiveFilter(qr, f)
	}
	for _, o := range q.Orders {
		qr = NaiveOrder(qr, o)
	}
	if q.Offset != 0 {
		qr = NaiveOffset(qr, q.Offset)
	}
	if q.Limit != 0 {
		qr = NaiveLimit(qr, q.Offset)
	}
	return qr
}

func ResultEntriesFrom(keys []string, vals []interface{}) []Entry {
	re := make([]Entry, len(keys))
	for i, k := range keys {
		re[i] = Entry{Key: k, Value: vals[i]}
	}
	return re
}
