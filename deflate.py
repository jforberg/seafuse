#!/usr/bin/python3

import zlib, sys

sys.stdout.buffer.write(zlib.decompress(sys.stdin.buffer.read()))
