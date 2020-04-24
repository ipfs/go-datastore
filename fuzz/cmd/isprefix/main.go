package main

// Checks if a db instance is equivalent to some prefix of an input script.

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
var db *string = pflag.StringP("db", "d", "go-ds-badger", "database driver")
var dbPrev *string = pflag.StringP("exist", "e", "tmp1", "database instance already made")
var dbFile *string = pflag.StringP("file", "f", "tmp2", "where the replay should live")
var threads *int = pflag.IntP("threads", "t", 1, "concurrent threads")

type validatingReader struct {
	b         []byte
	i         int
	validator func(bool) bool
	validI    int
}

func (v *validatingReader) Read(buf []byte) (n int, err error) {
	if v.i == len(v.b) {
		return 0, nil
	}
	if v.validator(false) {
		v.validI = v.i
	}
	buf[0] = v.b[v.i]
	v.i++
	return 1, nil
}

func main() {
	pflag.Parse()

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

	previousDB, err := fuzzer.Open(*db, *dbPrev, false)
	if err != nil {
		fmt.Fprintf(os.Stderr, "could not open: %v\n", err)
		return
	}
	defer previousDB.Cancel()

	replayDB, err := fuzzer.Open(*db, *dbFile, true)
	if err != nil {
		fmt.Fprintf(os.Stderr, "could not open: %v\n", err)
		return
	}
	defer replayDB.Cancel()

	reader := validatingReader{dat, 0, func(verbose bool) bool {
		res, _ := replayDB.DB().Query(dsq.Query{})
		for e := range res.Next() {
			if e.Entry.Key == "/" {
				continue
			}
			if h, _ := previousDB.DB().Has(ds.NewKey(e.Entry.Key)); !h {
				if verbose {
					fmt.Printf("failed - script run db has %s not in existing.\n", e.Entry.Key)
				}
				return false // not yet complete
			}
		}
		// next; make sure the other way is equal.
		res, _ = previousDB.DB().Query(dsq.Query{})
		for e := range res.Next() {
			if e.Entry.Key == "/" {
				continue
			}
			if h, _ := replayDB.DB().Has(ds.NewKey(e.Entry.Key)); !h {
				if verbose {
					fmt.Printf("failed - existing db has %s not in replay.\n", e.Entry.Key)
				}
				return false
			}
		}
		// db images are the same.
		return true
	}, -1}

	replayDB.FuzzStream(&reader)
	if reader.validator(true) {
		reader.validI = reader.i
	}

	if reader.validI > -1 {
		fmt.Printf("Matched at stream position %d.\n", reader.validI)
		os.Exit(0)
	} else {
		fmt.Printf("Failed to match\n")
		os.Exit(1)
	}
}
