package main

import (
	"fmt"
	"io/ioutil"
	"os"

	ds "github.com/ipfs/go-datastore"
	fuzzer "github.com/ipfs/go-datastore/fuzz"
	dsq "github.com/ipfs/go-datastore/query"

	"github.com/spf13/pflag"
)

var input *string = pflag.StringP("input", "i", "", "file to read input from (stdin used if not specified)")
var db1 *string = pflag.StringP("db1", "d", "badger", "database to fuzz")
var db2 *string = pflag.StringP("db2", "e", "level", "database to fuzz")
var dbFile *string = pflag.StringP("file", "f", "tmp", "where the db instances should live on disk")
var threads *int = pflag.IntP("threads", "t", 1, "concurrent threads")

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
		fmt.Fprintf(os.Stderr, "Could not read %s: %v\n", *input, err)
		return
	}

	db1loc := *dbFile + "1"
	inst1, err := fuzzer.Open(*db1, db1loc, false)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Could not open db: %v\n", err)
		return
	}
	defer inst1.Cancel()

	db2loc := *dbFile + "2"
	inst2, err := fuzzer.Open(*db2, db2loc, false)
	if err != nil {
		inst1.Cancel()
		fmt.Fprintf(os.Stderr, "Could not open db: %v\n", err)
		return
	}
	defer inst2.Cancel()

	fmt.Printf("Running db1.........")
	inst1.Fuzz(dat)
	fmt.Printf("done\n")
	fmt.Printf("Running db2.........")
	inst2.Fuzz(dat)
	fmt.Printf("done\n")

	fmt.Printf("Checking equality...")
	db1 := inst1.DB()
	db2 := inst2.DB()
	r1, err := db1.Query(dsq.Query{})
	if err != nil {
		panic(err)
	}

	for r := range r1.Next() {
		if r.Error != nil {
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

	for r := range r2.Next() {
		if r.Error != nil {
			break
		}
		if r.Entry.Key == "/" {
			continue
		}

		if exist, _ := db1.Has(ds.NewKey(r.Entry.Key)); !exist {
			fmt.Fprintf(os.Stderr, "db1 failed to get key %s held by db2\n", r.Entry.Key)
		}
	}

	fmt.Printf("Done\n")
}
