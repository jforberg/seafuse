#!/bin/sh

rm -r t
cargo run -v extract tests/testrepos/basic 868be3a7-b357-4189-af52-304b402d9904 t
