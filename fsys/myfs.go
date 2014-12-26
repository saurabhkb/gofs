package fsys

import (
	"bazil.org/fuse"
	"bazil.org/fuse/fs"
)

/*
======================
FILESYSTEM
=======================
*/


/* define custom filesystem with RootDir and Pid */
type MyFS struct{
	RootDir *MyNode
}

/* required to make MyFS implement `FS` interface */
func (f MyFS) Root() (fs.Node, fuse.Error) {
	return f.RootDir, nil
}
