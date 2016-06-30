package flatfs

import (
	"io/ioutil"
	"os"
	"math/rand"
	"time"
	"fmt"
	"path/filepath"
	"strings"

	ds "github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
)

var N = 10000

func TimeQuery() {
	rand.Seed(time.Now().UTC().UnixNano())
	path, err := ioutil.TempDir("/tmp", "flatdb_")
	if err != nil {
		panic(err)
	}

	d, err := New(path, 5, false)
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
	//TestDirect(d)
	//TestDirect(d)
	//TestDirect(d)
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
	for i := 0; i <= 100; i++ {
	 	dsq.KeysOnlyBufSize = 1 << uint(rand.Intn(15))
	 	if dsq.KeysOnlyBufSize <= 8192 {
	 		TestQuery(d)
	 	} else {
	 		TestDirect(d)
	 	}
	}
}

func TestQuery(d *Datastore) {
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
	fmt.Printf("query %d %f\n", dsq.KeysOnlyBufSize, elapsed.Seconds()*1000)
}

func TestDirect(fs *Datastore) {
	start := time.Now()
	i := 0
	err := filepath.Walk(fs.path, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			log.Errorf("Walk func in Query got error: %v", err)
			return err
		}

		if !info.Mode().IsRegular() || strings.HasPrefix(info.Name(), ".") {
			return nil
		}
		
		key, ok := fs.decode(info.Name())
		if !ok {
			log.Warning("failed to decode entry in flatfs")
			return nil
		}

		i += int(key.String()[0])

		return nil
	})
	if err != nil {
		log.Warning("walk failed: ", err)
	}
	elapsed := time.Since(start)
	//fmt.Printf("i = %d\n", i)
	fmt.Printf("direct %d %f\n", dsq.KeysOnlyBufSize, elapsed.Seconds()*1000)
	
}
	


func RandomString(strlen int) string {
	const chars = "abcdefghijklmnopqrstuvwxyz0123456789"
	result := make([]byte, strlen)
	for i := 0; i < strlen; i++ {
		result[i] = chars[rand.Intn(len(chars))]
	}
	return string(result)
}

