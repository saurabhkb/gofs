Project 2 Submission

NOTE
Makefile assumes that /tmp/mount = mount point, /tmp/db = location for database


RUNNING
Just run `make`


DESCRIPTION
There's a bunch of small files:

flusher.go
runs every few seconds and writes back to stable storage

control.go
controls everything, loads the filesystem into memory, provides an interface for storing data to stable storage, maintains state, lists of inodes, version ids

myfs.go
just a basic structure describing a filesystem

mynode.go
contains the functionality of a node

chunker.go
contains method for chunking based on Rabin-Karp

stableStorage.go
provides interface to write to stable storage independent of the actual database library used

main.go
contains the main method, calls functions from all the other files, sets everything running, closes everything on interrupt


ARCHIVING
Archive directories and their contents are read only. They can only be removed with the rmdir command.
