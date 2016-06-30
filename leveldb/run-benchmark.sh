#!/bin/sh

set -x

go run main/main.go > res
./plot.sh
