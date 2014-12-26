package main

import (
	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	_ "bazil.org/fuse/fs/fstestutil"
	"flag"
	"fmt"
	"os"
	"log"
	"os/signal"
	"p4/storage"
	"p4/util"
	"p4/lock"
	"p4/fsys"
)


/*
=======================
MAIN FUNCTION
=======================
*/

/* memfs implements a simple in-memory file system */


var Usage = func() {
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  %s MOUNTPOINT DB_LOCATION\n", os.Args[0])
	flag.PrintDefaults()
}

func main() {
	flag.Usage = Usage
	debugPtr := flag.Bool("debug", false, "print lots of stuff")
	namePtr := flag.String("name", "auto", "replica name")
	newfsPtr := flag.Bool("newfs", false, "reinitialize local filesystem")
	flag.Parse()

	util.SetDebug(*debugPtr)

	util.SetConfigFile(*namePtr)
	err, serverName, pid, mountpoint, dbpath, hostEndpoint := util.GetConfigDetailsFromName(*namePtr)

	if err != nil {
		log.Fatal(err)
	}
	storage.Init(dbpath)

	/* if newfs flag is true, clear storage */
	if *newfsPtr {
		storage.Clear()
	}

	lock.Init()

	/* unmount previously mounted filesystem (if any) */
	mounterr := os.MkdirAll(mountpoint, os.ModeDir | 0755)
	util.P_out("mount creation err: %v", mounterr)
	fuse.Unmount(mountpoint) //!!
	c, err := fuse.Mount(mountpoint)
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	endpointList := util.ReadAllEndpoints()
	fsys.Init(serverName, pid, mountpoint, dbpath, hostEndpoint)

	fsys.StartPub()
	fsys.StartRep()
	fsys.StartReq()

	/* create and initialize new custom filesystem */
	var MyFileSystem = fsys.MyFS{}
	fsys.SetMyPid(pid)
	fsys.LoadState()
	fsys.LoadFS(&MyFileSystem)
	fsys.StartSub(endpointList, &MyFileSystem)

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


	/* start the flusher */
	writeBackQuitter := make(chan bool)
	go fsys.Flush(writeBackQuitter, &MyFileSystem)


	/* gracefully end the system */
	sigchan := make(chan os.Signal)
	signal.Notify(sigchan, os.Interrupt)	// die on interrupt
	<-sigchan
	util.P_out("received interrupt")
	util.P_out("ending flusher (could take a few seconds)")
	writeBackQuitter <- true
	util.P_out("ending filesystem serve")
	util.P_out("In order to gracefully unmount, the filesystem should not be in use (otherwise it will block)")
	fuse.Unmount(mountpoint)
	c.Close()
	util.P_out("ended filesystem serve")
	os.Exit(0)
}
