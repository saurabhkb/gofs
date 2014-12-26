package main

import (
	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	_ "bazil.org/fuse/fs/fstestutil"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
)



/*
=======================
MAIN FUNCTION
=======================
*/

/* memfs implements a simple in-memory file system */

var Debug = true

/* Debug functions */
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
	fmt.Fprintf(os.Stderr, "  %s MOUNTPOINT DB_LOCATION\n", os.Args[0])
	flag.PrintDefaults()
}

func main() {
	flag.Usage = Usage
	debugPtr := flag.Bool("debug", false, "print lots of stuff")
	flag.Parse()
	Debug = *debugPtr

	if flag.NArg() != 2 {
		Usage()
		os.Exit(2)
	}

	/* get mount point and database location from user */
	mountpoint := flag.Arg(0)
	dbpath := flag.Arg(1)

	/* initialize the database */
	InitDatabase(dbpath)

	/* initialize the lock */
	InitLock()

	/* unmount previously mounted filesystem (if any) */
	fuse.Unmount(mountpoint) //!!
	c, err := fuse.Mount(mountpoint)
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	/* create and initialize new custom filesystem */
	var MyFileSystem = MyFS{}
	LoadState()
	LoadFS(&MyFileSystem)


	/* serve the filesystem from the mountpoint */
	go func() {
		err = fs.Serve(c, MyFileSystem)
		if err != nil {
			log.Fatal(err)
		}

		/* check if the mount process has an error to report */
		<-c.Ready
		if err := c.MountError; err != nil {
			log.Fatal(err)
		}
	}()


	writeBackQuitter := make(chan bool)
	var flusher Flusher

	/* start the flusher */
	go flusher.flush(writeBackQuitter, &MyFileSystem)


	/* gracefully end the system */
	sigchan := make(chan os.Signal)
	signal.Notify(sigchan, os.Interrupt)	// die on interrupt
	<-sigchan
	P_out("received interrupt")

	P_out("ending flusher (could take a few seconds)")
	writeBackQuitter <- true


	P_out("ending filesystem serve")
	P_out("In order to gracefully unmount, the filesystem should not be in use (otherwise it will block)")
	// unmount fuse
	fuse.Unmount(mountpoint)
	c.Close()
	P_out("ended filesystem serve")

	os.Exit(0)
}
