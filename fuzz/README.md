IPFS Datastore Fuzzer
====

The fuzzer provides a [go fuzzer](https://github.com/dvyukov/go-fuzz) interface
to Datastore implementations. This can be used for fuzz testing of these
implementations.

Usage
----

First, configure the datastores to fuzz (from this directory):
```golang
// either run via `go run`
go run ./cmd/generate github.com/ipfs/go-ds-badger
// or `go generate`
DS_PROVIDERS="github.com/ipfs/go-ds-badger" go generate
```

Then, build the fuzzing artifact and fuzz:
```golang
go-fuzz-build
go-fuzz
```

If you don't have `go-fuzz` installed, it can be acquired as:
```
go get -u github.com/dvyukov/go-fuzz/go-fuzz github.com/dvyukov/go-fuzz/go-fuzz-build
```
