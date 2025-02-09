#!/bin/sh

set -eux

cargo build

trap 'fusermount -u mnt' exit

target/debug/seafrepo --verbose mount tests/data/testrepo 868be3a7-b357-4189-af52-304b402d9904 mnt
