// memfs implements a simple in-memory file system.
package main

/*
 Two main files are ../fuse.go and ../fs/serve.go
*/

import (
	"flag"
	"fmt"
	"os"
	"log"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	_ "bazil.org/fuse/fs/fstestutil"
)


var Debug = true

/*
*	Debug functions
*/
func P_out(s string, args ...interface{}) {
	if !Debug {
		return
	}
	log.Printf(s, args...)
}

func P_err(s string, args ...interface{}) {
	log.Printf(s, args...)
}


var Usage = func() {
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  %s MOUNTPOINT\n", os.Args[0])
	flag.PrintDefaults()
}

func main() {
	flag.Usage = Usage
	debugPtr := flag.Bool("debug", false, "print lots of stuff")
	flag.Parse()
	Debug = *debugPtr

	P_out("main\n");

	if flag.NArg() != 1 {
		Usage()
		os.Exit(2)
	}

	// get mount point from user
	mountpoint := flag.Arg(0)

	// unmount previously mounted filesystem (if any)
	fuse.Unmount(mountpoint)		//!!
	c, err := fuse.Mount(mountpoint)
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	// create and initialize new custom filesystem
	var MyFileSystem = MyFS{}
	MyFileSystem.Init()

	// serve the filesystem from the mountpoint
	err = fs.Serve(c, MyFileSystem)
	if err != nil {
		log.Fatal(err)
	}

	// check if the mount process has an error to report
	<-c.Ready
	if err := c.MountError; err != nil {
		log.Fatal(err)
	}
}
