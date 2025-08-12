#!/bin/sh

set -eux

trap 'fusermount -u mnt' exit

cargo run mount -v tests/testrepos/multiblock 868be3a7-b357-4189-af52-304b402d9904 mnt

