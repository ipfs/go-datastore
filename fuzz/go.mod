module github.com/ipfs/go-datastore/fuzz

go 1.14

replace github.com/ipfs/go-datastore => ../

require (
	github.com/ipfs/go-datastore v0.4.4
	github.com/ipfs/go-ds-badger v0.2.4
	github.com/spf13/pflag v1.0.3
)
