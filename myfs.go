package main

import (
	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"os"
)

var root *MyNode

/* define custom filesystem */
type MyFS struct{}

/* initializes the custom filesystem with a root mynode */
func (MyFS) Init() {
	root = new(MyNode)
	root.Init("", os.ModeDir | 0755)
}

/* required to make MyFS implement `FS` interface */
func (MyFS) Root() (fs.Node, fuse.Error) {
	return root, nil
}

