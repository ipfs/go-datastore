package datastore

type GetCallback func(interface{}, error)

// basicTransaction implements the transaction interface for datastores who do
// not have any sort of underlying transactional support
type basicTransaction struct {
	puts    map[Key]interface{}
	gets    map[Key]GetCallback
	deletes map[Key]struct{}

	target Datastore
}

func newBasicTransaction(ds Datastore) Transaction {
	return &basicTransaction{
		puts:    make(map[Key]interface{}),
		gets:    make(map[Key]GetCallback),
		deletes: make(map[Key]struct{}),
		target:  ds,
	}
}

func (bt *basicTransaction) Put(key Key, val interface{}) error {
	bt.puts[key] = val
	return nil
}

func (bt *basicTransaction) Delete(key Key) error {
	bt.deletes[key] = struct{}{}
	return nil
}

func (bt *basicTransaction) Commit() error {
	for k, cb := range bt.gets {
		cb(bt.target.Get(k))
	}

	for k, val := range bt.puts {
		if err := bt.target.Put(k, val); err != nil {
			return err
		}
	}

	for k, _ := range bt.deletes {
		if err := bt.target.Delete(k); err != nil {
			return err
		}
	}

	return nil
}
