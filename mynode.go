package main

import (
	"os"
	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"fmt"
	"time"
	"strings"
	"syscall"
)

/*
======================
NODE
=======================
*/

/*
	MyNode
	Implements the Node interface (as it has a Attr() method which returns an object implementing Attr)
*/
type MyNode struct {

	NodeID int	/* unique ID in the face of versions and renames */

	Vid int
	Name string
	Attrib fuse.Attr

	dirty bool

	/*
		set only if the node is a directory corresponding to an archive.
		Note: the actual archive files/dirs themselves do not have this set
	*/
	archive bool

	expanded bool

	parent *MyNode

	children map[string]*MyNode
	ChildVids map[string]int

	data []byte

	/* For now, BlockOffsets and BlockLengths are exported fields in the MyNode structure. Can change later if required. */
	BlockOffsets []int
	BlockLengths []int
	DataBlocks []string
}

func (n *MyNode) String() string {
	return fmt.Sprintf("MyNode inode=%d, Vid=%d, name=%q, ChildVids=%v", n.Attr().Inode, n.Vid, n.Name, n.ChildVids)
}

/* whether this mynode is a directory */
func (n *MyNode) isDir() bool {
	return n.Attrib.Mode.IsDir()
}

/* returns type of this mynode */
func (n *MyNode) fuseType() fuse.DirentType {
	if n.isDir() {
		return fuse.DT_Dir
	} else {
		return fuse.DT_File
	}
}

/* initialization of this mynode */
func (n *MyNode) Init(name string, mode os.FileMode, parent *MyNode) {

	n.NodeID = GetAvailableUid()

	n.Attrib.Inode = GetAvailableInode()
	n.Attrib.Nlink = 1
	n.Name = name

	tm := time.Now()
	n.Attrib.Atime = tm
	n.Attrib.Mtime = tm
	n.Attrib.Ctime = tm
	n.Attrib.Crtime = tm
	n.Attrib.Mode = mode

	n.Attrib.Gid = uint32(os.Getegid())
	n.Attrib.Uid = uint32(os.Geteuid())

	n.Attrib.Size = 0

	n.parent = parent

	n.dirty = false

	n.archive = false

	n.children = make(map[string]*MyNode)
	n.ChildVids = make(map[string]int)


}

/* An Attr method to return the basic file attributes defined by Attr. Required to implement Node interface */
func (n *MyNode) Attr() fuse.Attr {
	return n.Attrib
}

/* checks whether a child with name `name` exists */
func (n *MyNode) Lookup(name string, intr fs.Intr) (fs.Node, fuse.Error) {
	assertExpanded(n)
	if k, ok := n.children[name]; ok {
		return k, nil
	}
	return nil, fuse.ENOENT
}

/* reads directory. */
func (n *MyNode) ReadDir(intr fs.Intr) ([]fuse.Dirent, fuse.Error) {
	LOCK.Lock()
	defer LOCK.Unlock()
	assertExpanded(n)
	dirs := make([]fuse.Dirent, 0, 10)
	for k, v := range n.children {
		dirs = append(dirs, fuse.Dirent{Inode: v.Attrib.Inode, Name: k, Type: v.fuseType()})
	}
	return dirs, nil
}

/* must be defined or editing w/ vi or emacs fails. Doesn't have to do anything */
func (n *MyNode) Fsync(req *fuse.FsyncRequest, intr fs.Intr) fuse.Error {
	LOCK.Lock()
	defer LOCK.Unlock()
	return nil
}

/* creates a directory */
func (p *MyNode) Mkdir(req *fuse.MkdirRequest, intr fs.Intr) (fs.Node, fuse.Error) {
	LOCK.Lock()
	defer LOCK.Unlock()
	assertExpanded(p)

	if !isAllowed(p, "w") {
		return nil, fuse.Errno(syscall.EACCES)
	}

	if strings.Contains(req.Name, "@") {
		tokens := strings.Split(req.Name, "@")
		filename := tokens[0]

		/* this assumes that it hasn't been deleted */
		trial, found := p.children[filename]

		var n *MyNode = nil
		if !found {
			/* if it has been deleted, look at old versions of the parent to see if we can find it */
			/* NOTE: currently, even if the node has been moved somewhere else, this will still allow the archive to be created */
			P_out("looking through old versions of parent")
			parentVersions := GetNodeVersions(p.NodeID)
			for i := len(parentVersions) - 1; i >= 0; i-- {
				vnode := LoadNodeVersion(parentVersions[i])
				vid, err := vnode.ChildVids[filename]
				if err {
					/* version found */
					n = LoadNodeVersion(vid)
					break
				}
			}
		} else {
			n = trial
		}

		if(n == nil) {
			return nil, fuse.Errno(syscall.ENOENT)
		}

		if n.isDir() {
			date := tokens[1]
			finalTime := parseTime(date)
			P_out("final time: %v", finalTime)
			versions := GetNodeVersions(n.NodeID)
			var prev *MyNode = nil
			for i := 0; i < len(versions); i++ {
				vnode := LoadNodeVersion(versions[i])
				if vnode.Attrib.Mtime.Before(finalTime) {
					prev = vnode
				} else {
					break
				}
			}
			if prev != nil {
				/* copy all of prev's children into d */
				d := new(MyNode)
				d.Init(req.Name, os.ModeDir|0555, p)
				p.children[req.Name] = d
				for k, v := range prev.ChildVids {
					d.ChildVids[k] = v
				}
				P_out("state of children: %v", d.ChildVids)
				d.archive = true
				return d, nil
			} else {
				/* trying to get a version from before the folder was actually created */
				return nil, fuse.EPERM
			}
		} else {
			d := new(MyNode)
			d.Init(req.Name, os.ModeDir|0444, p)
			p.children[req.Name] = d
			versions := GetNodeVersions(n.NodeID)
			for i := 0; i < len(versions); i++ {
				vnode := LoadNodeVersion(versions[i])
				vnode.Name = vnode.Name + ".[" + vnode.Attrib.Mtime.Format("Mon Jan 2 15:04:05 -0700 MST 2006") + "]"
				vnode.Attrib.Mode = vnode.Attrib.Mode & 0444;	/* make it read-only */
				P_out("vnode: %v", vnode.Attrib.Mtime)
				d.children[vnode.Name] = vnode
			}
			d.expanded = true
			d.archive = true
			return d, nil
		}


	} else {
		d := new(MyNode)
		d.Init(req.Name, os.ModeDir|0755, p)
		p.children[req.Name] = d

		updateAncestors(d)

		d.Attrib.Mtime = time.Now()
		current := d.parent
		for current != nil {
			current.Attrib.Mtime = d.Attrib.Mtime
			current = current.parent
		}

		return d, nil
	}
}

/* creates a file */
func (p *MyNode) Create(req *fuse.CreateRequest, resp *fuse.CreateResponse, intr fs.Intr) (fs.Node, fs.Handle, fuse.Error) {
	LOCK.Lock()
	defer LOCK.Unlock()
	assertExpanded(p)
	if !isAllowed(p, "w") {
		return nil, nil, fuse.Errno(syscall.EACCES)
	}
	f := new(MyNode)
	f.Init(req.Name, req.Mode, p)
	p.children[req.Name] = f
	return f, f, nil
}

/* removes a file */
func (p *MyNode) Remove(req *fuse.RemoveRequest, intr fs.Intr) fuse.Error {
	LOCK.Lock()
	defer LOCK.Unlock()
	assertExpanded(p)

	if !isAllowed(p, "w") {
		return fuse.Errno(syscall.EACCES)
	}


	P_out("remove: %s", p.Name)
	child, ok := p.children[req.Name] /* child is to be deleted */

	/* update the structure and remove the subtree rooted here */
	if child.archive {
		P_out("special case of rmdir: removing archive")
		delete(p.children, req.Name)
		child = nil
		return nil
	}

	if !ok {
		P_out("invalid file or directory!")
		return fuse.ENOENT
	} else {
		performDelete := (req.Dir && child.isDir() && len(child.ChildVids) == 0) || (!req.Dir && !child.isDir())

		if performDelete {
			delete(p.children, req.Name)
			delete(p.ChildVids, req.Name)
			updateAncestors(p)
			child = nil
		} else {
			return fuse.Errno(syscall.EPERM)
		}
	}
	return nil
}

/* write to a file */
func (n *MyNode) Write(req *fuse.WriteRequest, resp *fuse.WriteResponse, intr fs.Intr) fuse.Error {
	LOCK.Lock()
	defer LOCK.Unlock()
	assertExpanded(n)
	if !isAllowed(n, "w") {
		return fuse.Errno(syscall.EACCES)
	}

	/* helper variable to avoid all the typecasts */
	dataLen64 := int64(len(req.Data))

	/* extend if necessary */
	if int64(cap(n.data)) < req.Offset + dataLen64 {
		t := make([]byte, req.Offset + dataLen64)
		copy(t, n.data)
		n.data = t
	}
	if len(n.data) < cap(n.data) {
		n.data = n.data[:cap(n.data)]
	}

	/* copy out the data */
	for i := 0; i < len(req.Data); i++ {
		n.data[int(req.Offset) + i] = req.Data[i]
	}

	/* Did the write go past the current size of the file? Or did the write stay within the file bounds? */
	if uint64(dataLen64 + req.Offset) > n.Attrib.Size {
		n.Attrib.Size = uint64(req.Offset + dataLen64)
	}

	/* how many bytes were written out (crucial, otherwise applications think that nothing was written out and get pissed) */
	resp.Size = len(req.Data)

	/* update modified time */
	n.Attrib.Mtime = time.Now()
	current := n.parent
	for current != nil {
		current.Attrib.Mtime = n.Attrib.Mtime
		current = current.parent
	}

	/* all sorts of new stuff to be done here for persistence */

	/* update version number */
	n.DataBlocks, n.BlockOffsets, n.BlockLengths = ChunkifyAndStoreRK(n.data)

	/* TODO update ancestors' versions */
	updateAncestors(n)

	return nil
}

/* read from a file */
func (n *MyNode) ReadAll(intr fs.Intr) ([]byte, fuse.Error) {
	LOCK.Lock()
	defer LOCK.Unlock()
	if !isAllowed(n, "r") {
		P_out("CANT read stuff from %s", n.Name)
		return nil, fuse.Errno(syscall.EACCES)
	}
	assertExpanded(n)
	P_out("reading stuff size=%d, from %s", n.Attrib.Size, n.Name)
	P_out("datablocks: %v", n.DataBlocks)
	P_out("data: %s", n.data)
	return n.data[:n.Attrib.Size], nil
}

/*
	Flush a file
	This is called on a file whenever that file descriptor is closed
	(There is no guarantee that it will be called after file writes)
*/
func (n *MyNode) Flush(req *fuse.FlushRequest, intr fs.Intr) fuse.Error {
	LOCK.Lock()
	defer LOCK.Unlock()
	return nil
}

/* rename a file (p = parent node) */
func (p *MyNode) Rename(req *fuse.RenameRequest, newDir fs.Node, intr fs.Intr) fuse.Error {
	LOCK.Lock()
	defer LOCK.Unlock()
	assertExpanded(p)

	if !isAllowed(p, "w") {
		return fuse.Errno(syscall.EACCES)
	}

	/* remove child from current parent */
	childToRename := p.children[req.OldName]

	if childToRename.archive {
		return fuse.Errno(syscall.EPERM)
	}

	delete(p.children, req.OldName)
	delete(p.ChildVids, req.OldName)
	updateAncestors(p)

	/* attach to new parent, passed newDir better be a *MyNode */
	newParent, ok := newDir.(*MyNode)
	assertExpanded(newParent)
	if !ok {
		return fuse.EIO
	}
	newParent.children[req.NewName] = childToRename
	childToRename.Name = req.NewName
	childToRename.parent = newParent
	updateAncestors(childToRename)

	return nil
}

/* get file attributes */
func (n *MyNode) Getattr(req *fuse.GetattrRequest, resp *fuse.GetattrResponse, intr fs.Intr) fuse.Error {
	assertExpanded(n)
	resp.Attr = n.Attrib
	return nil
}

/* implementing this otherwise can't set permissions */
func (n *MyNode) Setattr(req *fuse.SetattrRequest, resp *fuse.SetattrResponse, intr fs.Intr) fuse.Error {
	LOCK.Lock()
	defer LOCK.Unlock()

	if !isAllowed(n, "w") {
		P_out("set attr is not allowed for %s", n.Name)
		return fuse.Errno(syscall.EACCES)
	}

	if req.Valid.Mode() {
		n.Attrib.Mode = req.Mode
	}
	if req.Valid.Atime() {
		n.Attrib.Atime = req.Atime
	}
	if req.Valid.Mtime() {
		n.Attrib.Mtime = req.Mtime
	}
	if req.Valid.Gid() {
		n.Attrib.Gid = req.Gid
	}
	if req.Valid.Uid() {
		n.Attrib.Uid = req.Uid
	}
	if req.Valid.Size() {
		n.Attrib.Size = req.Size
	}
	updateAncestors(n)

	return nil
}


/* utility function */
func updateAncestors(node *MyNode) {
	current := node
	for current != nil {
		current.Vid = GetAvailableVersionId()
		//RegisterNodeVersion(current.NodeID, current.Vid)  <== should happen in flusher (otherwise you get node versions without corresponding saved node)
		if current.parent != nil {
			current.parent.ChildVids[current.Name] = current.Vid
		}
		SaveNodeVersion(current)
		current = current.parent
	}
}

func parseTime(date string) time.Time {
	t, boolerr := peteTime(date)
	var finalTime time.Time
	if boolerr {
		/* its of the form: foo@-1m */
		duration, _ := time.ParseDuration(date)
		finalTime = time.Now().Add(duration)
		P_out("duration based final time: %v", finalTime)
	} else {
		/* its of the form: foo@2014-09-18 9:20 */
		finalTime = t
	}
	return finalTime
}

func peteTime(s string) (time.Time, bool) {
	timeFormats := []string{
		"2006-1-2 15:04:05",
		"2006-1-2 15:04",
		"2006-1-2",
		"1-2-2006 15:04:05",
		"1-2-2006 15:04",
		"1-6-2006",
		"2006/1/2 15:04:05",
		"2006/1/2 15:04",
		"2006/1/2",
		"1/2/2006 15:04:05",
		"1/2/2006 15:04",
		"1/2/2006",
	}
	loc, _ := time.LoadLocation("Local")

	for _,v := range timeFormats {
		if tm, terr := time.ParseInLocation(v, s, loc); terr == nil {
			return tm, false
		}
	}
	return time.Time{}, true
}

/*rwxrwxrwx
010010010
1 0010 0100
124
*/

func isAllowed(node *MyNode, operation string) bool {
	switch(operation) {
		case "w": {
			current := node.parent
			for current != nil {
				if current.archive {
					return false
				}
				current = current.parent
			}
			return node.Attrib.Mode.Perm() & 0x00000092 > 0
		}
		case "r": {
			return node.Attrib.Mode.Perm() & 0x00000124 > 0
		}
	}
	return false
}
