package main

import (
	"fmt"
	"io/ioutil"
	"os"

	ds "github.com/ipfs/go-datastore"
	fuzzer "github.com/ipfs/go-datastore/fuzz"
	dsq "github.com/ipfs/go-datastore/query"
	badger "github.com/ipfs/go-ds-badger"
	leveldb "github.com/ipfs/go-ds-leveldb"

	"github.com/spf13/pflag"
)

var input *string = pflag.StringP("input", "i", "", "file to read input from (stdin used if not specified)")
var db1 *string = pflag.StringP("db1", "d", "badger", "database to fuzz")
var db2 *string = pflag.StringP("db2", "e", "level", "database to fuzz")
var dbFile *string = pflag.StringP("file", "f", "tmp", "where the db instaces should live on disk")
var threads *int = pflag.IntP("threads", "t", 1, "concurrent threads")

func openDB(db string) {
	if db == "badger" {
		fuzzer.DsOpener = func() (ds.TxnDatastore, fuzzer.Donefunc) {
			d, err := badger.NewDatastore(*dbFile, &badger.DefaultOptions)
			if err != nil {
				panic("could not create db instance")
			}
			donefunc := func() error { return nil }
			return d, donefunc
		}
	} else if db == "level" {
		fuzzer.DsOpener = func() (ds.TxnDatastore, fuzzer.Donefunc) {
			d, err := leveldb.NewDatastore(*dbFile, &leveldb.Options{})
			if err != nil {
				panic("could not create db instance")
			}
			donefunc := func() error { return nil }
			return d, donefunc
		}
	} else {
		// TODO
		panic("unknown database")
	}
}

func main() {
	pflag.Parse()

	// do one, then the other, then compare state.

	fuzzer.Threads = *threads

	var dat []byte
	var err error
	if *input == "" {
		dat, err = ioutil.ReadAll(os.Stdin)
	} else {
		dat, err = ioutil.ReadFile(*input)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "could not read %s: %v\n", *input, err)
		return
	}

	fmt.Printf("running db 1\n")
	base := *dbFile
	*dbFile = base + "1"
	fuzzer.RandSeed(0)
	openDB(*db1)
	ret := fuzzer.Fuzz(dat)

	db1, _ := fuzzer.DsOpener()

	fmt.Printf("running db 2\n")
	*dbFile = base + "2"
	fuzzer.RandSeed(0)
	openDB(*db2)
	ret = fuzzer.Fuzz(dat)

	db2, _ := fuzzer.DsOpener()
	fmt.Printf("done\n")
	// compare.
	r1, err := db1.Query(dsq.Query{})
	if err != nil {
		panic(err)
	}

	fmt.Printf("checking equality\n")
	for r := range r1.Next() {
		if r.Error != nil {
			// handle.
			break
		}
		if r.Entry.Key == "/" {
			continue
		}

		if exist, _ := db2.Has(ds.NewKey(r.Entry.Key)); !exist {
			fmt.Fprintf(os.Stderr, "db2 failed to get key %s held by db1\n", r.Entry.Key)
		}
	}

	r2, err := db2.Query(dsq.Query{})
	if err != nil {
		panic(err)
	}

	fmt.Printf("checking equality\n")
	for r := range r2.Next() {
		if r.Error != nil {
			// handle.
			break
		}
		if r.Entry.Key == "/" {
			continue
		}

		if exist, _ := db1.Has(ds.NewKey(r.Entry.Key)); !exist {
			fmt.Fprintf(os.Stderr, "db1 failed to get key %s held by db2\n", r.Entry.Key)
		}
	}

	db1.Close()
	db2.Close()
	fmt.Printf("done\n")

	os.Exit(ret)
}
