package main

import (
	"os"
	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"fmt"
	"time"
)

/*
======================
NODE
=======================
*/

var uid = os.Geteuid()
var gid = os.Getegid()

/*
	MyNode
	Implements the Node interface (as it has a Attr() method which returns an object implementing Attr)
*/
type MyNode struct {
	Vid int
	Name string
	Attrib fuse.Attr

	dirty bool

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
	P_out("INIT: inited node inode %d, %q and len = %d\n", n.Attrib.Inode, name, len(n.children))
	P_out("INIT: number of children for %q is %d\n", name, len(n.children))
	n.Attrib.Inode = GetAvailableInode()
	n.Attrib.Nlink = 1
	n.Name = name

	tm := time.Now()
	n.Attrib.Atime = tm
	n.Attrib.Mtime = tm
	n.Attrib.Ctime = tm
	n.Attrib.Crtime = tm
	n.Attrib.Mode = mode

	n.Attrib.Gid = uint32(gid)
	n.Attrib.Uid = uint32(uid)

	n.Attrib.Size = 0

	n.parent = parent

	n.dirty = false

	n.children = make(map[string]*MyNode)
	n.ChildVids = make(map[string]int)


}

/* An Attr method to return the basic file attributes defined by Attr. Required to implement Node interface */
func (n *MyNode) Attr() fuse.Attr {
	LOCK.Lock()
	defer LOCK.Unlock()
	return n.Attrib
}

/* checks whether a child with name `name` exists */
func (n *MyNode) Lookup(name string, intr fs.Intr) (fs.Node, fuse.Error) {
	P_out("LOOKUP: looking up %s in %s", name, n.Name)
	assertExpanded(n)
	if k, ok := n.children[name]; ok {
		return k, nil
	}
	return nil, fuse.ENOENT
}

/* reads directory. */
func (n *MyNode) ReadDir(intr fs.Intr) ([]fuse.Dirent, fuse.Error) {
	P_out("READING DIR: %s", n.Name)
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
	//P_out("FSYNC: %v", req)
	LOCK.Lock()
	defer LOCK.Unlock()
	return nil
}

/* creates a directory */
func (p *MyNode) Mkdir(req *fuse.MkdirRequest, intr fs.Intr) (fs.Node, fuse.Error) {
	//P_out("MKDIR: %q in %q\n", req.Name, p.Name)
	LOCK.Lock()
	defer LOCK.Unlock()
	assertExpanded(p)
	d := new(MyNode)
	d.Init(req.Name, os.ModeDir|0755, p)
	p.children[req.Name] = d

	current := d
	for current != nil {
		current.Vid = GetAvailableVersionId()
		if current.parent != nil {
			current.parent.ChildVids[current.Name] = current.Vid
		}
		SaveNodeVersion(current)
		current = current.parent
	}

	return d, nil
}

/* creates a file */
func (p *MyNode) Create(req *fuse.CreateRequest, resp *fuse.CreateResponse, intr fs.Intr) (fs.Node, fs.Handle, fuse.Error) {
	//P_out("CREATE: %q in %q\n with mode %v", req.Name, p.Name, req.Mode)
	//P_out("CREATE: response: %v", resp)
	LOCK.Lock()
	defer LOCK.Unlock()
	assertExpanded(p)
	f := new(MyNode)
	f.Init(req.Name, req.Mode, p)
	p.children[req.Name] = f
	// p.ChildVids[req.Name] = f.Vid <- cant create Vid entry here, no Vid has been assigned yet! (it gets assigned in Write!)

	/*
		nothing specified in the Handle interface, so just return same mynode object.
		mynode has method ReadAll and hence implements interface HandleReadAller
	*/
	return f, f, nil
}

/* removes a file */
func (p *MyNode) Remove(req *fuse.RemoveRequest, intr fs.Intr) fuse.Error {
	LOCK.Lock()
	defer LOCK.Unlock()
	assertExpanded(p)
	child, ok := p.children[req.Name] /* child is to be deleted */
	if !ok {
		P_out("invalid file or directory!")
		return fuse.ENOENT
	} else {
		performDelete := (req.Dir && child.isDir() && len(child.children) == 0) || (!req.Dir && !child.isDir())

		if performDelete {
			delete(p.children, req.Name)
			delete(p.ChildVids, req.Name)
			updateAncestors(p)
			child = nil
		}
	}
	return nil
}

/* write to a file */
func (n *MyNode) Write(req *fuse.WriteRequest, resp *fuse.WriteResponse, intr fs.Intr) fuse.Error {
	LOCK.Lock()
	defer LOCK.Unlock()
	assertExpanded(n)

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
	P_out("[%v], len=%d, cap=%d, offset=%d, len(req.Data)=%d\n", string(n.data), len(n.data), cap(n.data), req.Offset, len(req.Data))
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
	assertExpanded(n)
	return n.data[:n.Attrib.Size], nil
}

/*
	Flush a file
	This is called on a file whenever that file descriptor is closed
	(There is no guarantee that it will be called after file writes)
*/
func (n *MyNode) Flush(req *fuse.FlushRequest, intr fs.Intr) fuse.Error {
	//P_out("FLUSH: %q\n", n.Name)
	LOCK.Lock()
	defer LOCK.Unlock()
	return nil
}

/* rename a file (p = parent node) */
func (p *MyNode) Rename(req *fuse.RenameRequest, newDir fs.Node, intr fs.Intr) fuse.Error {
	//P_out("RENAME: newDirNid=%v, newDir=[%v], oldname=%s, newname=%s\n", req.NewDir, newDir, req.OldName, req.NewName)
	//P_out("RENAME: this node: %v", p)
	LOCK.Lock()
	defer LOCK.Unlock()
	assertExpanded(p)

	/* remove child from current parent */
	childToRename := p.children[req.OldName]
	delete(p.children, req.OldName)
	delete(p.ChildVids, req.OldName)
	updateAncestors(p)

	/* attach to new parent, passed newDir better be a *MyNode */
	newParent, ok := newDir.(*MyNode)
	assertExpanded(newParent)
	if !ok {
		return fuse.EIO
	}
	//P_out("RENAME: new parent mynode: %v", newParent)
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
		if current.parent != nil {
			current.parent.ChildVids[current.Name] = current.Vid
		}
		SaveNodeVersion(current)
		current = current.parent
	}
}
