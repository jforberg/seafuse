#!/bin/sh

set -eux

rm -rf t
cargo run -- -v extract --prefix somedir tests/testrepos/basic 868be3a7-b357-4189-af52-304b402d9904 t
