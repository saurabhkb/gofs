package main

import (
	"bazil.org/fuse"
	"bazil.org/fuse/fs"
)

/*
======================
FILESYSTEM
=======================
*/

var root *MyNode

/* define custom filesystem */
type MyFS struct{
	RootDir *MyNode
}

/* required to make MyFS implement `FS` interface */
func (f MyFS) Root() (fs.Node, fuse.Error) {
	return f.RootDir, nil
}
