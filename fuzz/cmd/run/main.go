package main

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"os"

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
	fuzzer.SetOpener(*db, *dbFile, false)

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
		fuzzer.Cleanup()
		os.Exit(ret)
	}
}
