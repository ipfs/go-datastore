module github.com/ipfs/go-datastore/fuzz

go 1.24

require (
	github.com/ipfs/go-datastore v0.8.2
	github.com/ipfs/go-ds-flatfs v0.5.3
	github.com/spf13/pflag v1.0.6
)

require (
	github.com/alexbrainman/goissue34681 v0.0.0-20191006012335-3fc7a47baff5 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/ipfs/go-log/v2 v2.5.1 // indirect
	github.com/mattn/go-isatty v0.0.17 // indirect
	go.uber.org/atomic v1.10.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.uber.org/zap v1.24.0 // indirect
	golang.org/x/sys v0.4.0 // indirect
)

replace github.com/ipfs/go-datastore => ../
