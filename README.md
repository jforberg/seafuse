# Seafuse

A standalone tool which can read the internal storage format of
[Seafile](https://www.seafile.com). Files can be extracted direcly, or mounted
as a read-only
[FUSE](https://www.kernel.org/doc/html/next/filesystems/fuse.html) filesystem
for on-demand use.

## Usage

To extract all files from a library:

    seafuse extract path/to/library/storage library-uuid target-dir

For example:

    seafuse extract /srv/seafile/seafile-data/storage 868be3a7-b357-4189-af52-304b402d9904 t

To mount as a FUSE filesystem:

    seafuse extract path/to/library/storage library-uuid mountpoint
