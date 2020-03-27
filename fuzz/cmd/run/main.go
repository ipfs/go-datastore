package main

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"os"

	ds "github.com/ipfs/go-datastore"
	badger "github.com/ipfs/go-ds-badger"
	fuzzer "github.com/ipfs/go-datastore/fuzz"

	"github.com/spf13/pflag"
)

var input *string = pflag.StringP("input", "i", "", "file to read input from (stdin used if not specified)")
var db *string = pflag.StringP("database", "d", "badger", "database to fuzz")
var dbFile *string = pflag.StringP("file", "f", "tmp", "where the db instace should live on disk")
var threads *int = pflag.IntP("threads", "t", 1, "concurrent threads")

func main() {
	pflag.Parse()

	fuzzer.Threads = *threads
	if *db == "badger" {
		fuzzer.DsOpener = func() ds.TxnDatastore {
			d, err := badger.NewDatastore(*dbFile, &badger.DefaultOptions)
			if err != nil {
				panic("could not create db instance")
			}
			return d
		}
	} else {
		// TODO
		panic("unknown database")
	}

	if *input != "" {
		dat, err := ioutil.ReadFile(*input)
		if err != nil {
			fmt.Fprintf(os.Stderr, "could not read %s: %v\n", *input, err)
			return
		}
		ret := fuzzer.Fuzz(dat)
		os.Exit(ret)
	} else {
		ret := fuzzer.FuzzStream(bufio.NewReader(os.Stdin))
		os.Exit(ret)
	}
}
