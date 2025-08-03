#!/usr/bin/python3

import zlib, sys

sys.stdout.buffer.write(zlib.compress(sys.stdin.buffer.read()))
