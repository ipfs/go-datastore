package leveldb

import (
	"io/ioutil"
	"os"
	"math/rand"
	"time"
	"fmt"

	ds "github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
)

var N = 100000

func TimeQuery() {
	rand.Seed(time.Now().UTC().UnixNano())
	path, err := ioutil.TempDir("/tmp", "leveldb_")
	if err != nil {
		panic(err)
	}

	d, err := NewDatastore(path, nil)
	if err != nil {
		panic(err)
	}
	defer func() {
		os.RemoveAll(path)
		d.Close()
	}()
	for n := 0; n < N; n++ {
		err := d.Put(ds.NewKey(RandomString(52)), []byte(RandomString(1000)))
		if err != nil {
			panic(err)
		}
	}
	//TestQuery(d)
	//TestQuery(d)
	//TestQuery(d)
	// TestDirect(d)
	// TestIterator(d)
	// TestQuery(d)
	// TestDirect(d)
	// TestQuery(d)
	// TestDirect(d)
	// TestQuery(d)
	// TestDirect(d)
	// TestQuery(d)
	// TestDirect(d)
	// TestDirect(d)
	// TestDirect(d)
	// TestIterator(d)
	// TestIterator(d)
	// TestIterator(d)
	// TestIterator(d)
	// TestQuery(d)
	// TestQuery(d)
	// TestQuery(d)
	for i := 0; i <= 1000; i++ {
	 	dsq.KeysOnlyBufSize = 1 << uint(rand.Intn(15))
	 	if dsq.KeysOnlyBufSize <= 8192 {
	 		TestQuery(d)
	 	} else {
	 		TestDirect(d)
	 	}
	}
}

func TestDirect(d *datastore) {
	start := time.Now()
	iter := d.DB.NewIterator(nil, nil)
	i := 0
	for iter.Next() {
		i += int(iter.Key()[0])
	}
	elapsed := time.Since(start)
	//fmt.Printf("i = %d\n", i)
	fmt.Printf("direct %d %f\n", dsq.KeysOnlyBufSize, elapsed.Seconds() * 1000)
}

func TestIterator(d *datastore) {
	start := time.Now()
	iter := d.Iterate("")
	i := 0
	for iter.Next() {
		i += int(iter.Key()[0])
	}
	elapsed := time.Since(start)
	//fmt.Printf("i = %d\n", i)
	fmt.Printf("iterator time = %s\n", elapsed)
}

func TestQuery(d *datastore) {
	start := time.Now()
	rs, err := d.Query(dsq.Query{KeysOnly: true})
	if err != nil {
		panic(err)
	}
	i := 0
	for r := range rs.Next() {
		i += int(r.Key[0])
	}
	elapsed := time.Since(start)
	//fmt.Printf("i = %d\n", i)
	fmt.Printf("query %d %f\n", dsq.KeysOnlyBufSize, elapsed.Seconds() * 1000)
}

func RandomString(strlen int) string {
	const chars = "abcdefghijklmnopqrstuvwxyz0123456789"
	result := make([]byte, strlen)
	for i := 0; i < strlen; i++ {
		result[i] = chars[rand.Intn(len(chars))]
	}
	return string(result)
}

