# btreedb
A simple B+ Tree key-value store that uses the append-only B+ Tree by Martin Hedenfalk.

The difference between LMDB and this is that LMDB doesn't play nice on systems without the unified page and buffer caches, like OpenBSD.
This implementation doesn't suffer from the same issues as it doesn't use memory mapped files.
