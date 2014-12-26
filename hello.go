// Hellofs implements a simple "hello world" file system.
package main

import (
	"flag"	// me:command line flag parsing
	"fmt"	// me:stdio of go
	"log"	// me:logging
	"os"	// me:interface to OS functionality

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	_ "bazil.org/fuse/fs/fstestutil"
)

// me:function for printing the help stuff
var Usage = func() {
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  %s MOUNTPOINT\n", os.Args[0])
	flag.PrintDefaults()
}

func main() {
	// me:prints the help stuff
	flag.Usage = Usage
	// me:performs parse of command line options
	flag.Parse()

	if flag.NArg() != 1 {
		Usage()
		os.Exit(2)
	}
	mountpoint := flag.Arg(0)

	// mounts a new FUSE connection on the named directory and returns a connection for reading/writing FUSE messages
	c, err := fuse.Mount(mountpoint)
	if err != nil {
		log.Fatal(err)
	}
	// me:go defer -> executed in LIFO when surrounding function returns (good housekeeping)
	defer c.Close()

	err = fs.Serve(c, FS{})
	if err != nil {
		log.Fatal(err)
	}

	// check if the mount process has an error to report
	<-c.Ready
	if err := c.MountError; err != nil {
		log.Fatal(err)
	}
}

// FS implements the hello world file system.
type FS struct{}

func (FS) Root() (fs.Node, fuse.Error) {
	return Dir{}, nil
}

// Dir implements both Node and Handle for the root directory.
type Dir struct{}

func (Dir) Attr() fuse.Attr {
	return fuse.Attr{Inode: 1, Mode: os.ModeDir | 0555}
}

func (Dir) Lookup(name string, intr fs.Intr) (fs.Node, fuse.Error) {
	if name == "hello" {
		return File{}, nil
	}
	return nil, fuse.ENOENT
}

var dirDirs = []fuse.Dirent{
	{Inode: 2, Name: "hello", Type: fuse.DT_File},
}

func (Dir) ReadDir(intr fs.Intr) ([]fuse.Dirent, fuse.Error) {
	return dirDirs, nil
}

// File implements both Node and Handle for the hello file.
type File struct{}

const greeting = "hello, world\n"

func (File) Attr() fuse.Attr {
	return fuse.Attr{Inode: 2, Mode: 0444, Size: uint64(len(greeting))}
}

func (File) ReadAll(intr fs.Intr) ([]byte, fuse.Error) {
	return []byte(greeting), nil
}
