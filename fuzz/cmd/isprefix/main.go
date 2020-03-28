package main

// sees if a db instance is equivalent to some prefix of running an input script.

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
var db *string = pflag.StringP("db", "d", "badger", "database driver")
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
	} else {
		if v.validator(false) {
			v.validI = v.i
		}
		buf[0] = v.b[v.i]
		v.i++
		return 1, nil
	}
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

	fuzzer.SetOpener(*db, *dbPrev, false)
	db1, _ := fuzzer.DsOpener()

	fuzzer.SetOpener(*db, *dbFile, false)

	fuzzer.RandSeed(0)

	reader := validatingReader{dat, 0, func(verbose bool) bool {
		res, _ := fuzzer.GetInst().Query(dsq.Query{})
		for e := range res.Next() {
			if e.Entry.Key == "/" {
				continue
			}
			if h, _ := db1.Has(ds.NewKey(e.Entry.Key)); !h {
				if verbose {
					fmt.Printf("failed - script run db has %s not in existing.\n", e.Entry.Key)
				}
				return false // not yet complete
			}
		}
		// next; make sure the other way is equal.
		res, _ = db1.Query(dsq.Query{})
		for e := range res.Next() {
			if e.Entry.Key == "/" {
				continue
			}
			if h, _ := fuzzer.GetInst().Has(ds.NewKey(e.Entry.Key)); !h {
				if verbose {
					fmt.Printf("failed - existing db has %s not in replay.\n", e.Entry.Key)
				}
				return false
			}
		}
		// db images are the same.
		return true
	}, -1}
	fuzzer.FuzzStream(&reader)
	if reader.validator(true) {
		reader.validI = reader.i
	}
	fuzzer.Cleanup()

	db1.Close()

	if reader.validI > -1 {
		fmt.Printf("Matched at stream position %d.\n", reader.validI)
		os.Exit(0)
	} else {
		fmt.Printf("Failed to match\n")
		os.Exit(1)
	}
}
