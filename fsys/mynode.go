package fsys

import (
	"os"
	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"fmt"
	"time"
	"strings"
	"syscall"
	"p4/lock"
	"p4/util"
	"p4/storage"
)

/*
======================
NODE
=======================
*/

type Stub struct {
	NodeID int
	Vid string
	Name string
	Attrib fuse.Attr
	LastWriter int
}

func (s *Stub) String() string {
	return fmt.Sprintf("Stub: %s => mode: %v, lastWriter: %d", s.Name, s.Attrib.Mode, s.LastWriter)
}


/*
	MyNode
	Implements the Node interface (as it has a Attr() method which returns an object implementing Attr)
*/
type MyNode struct {

	NodeID int	/* unique ID in the face of versions and renames */

	Vid string
	Name string
	Attrib fuse.Attr

	dirty bool

	/* set only if the node is a directory corresponding to an archive. the actual archive files/dirs themselves do not have this set */
	archive bool

	expanded bool

	parent *MyNode

	children map[string]*MyNode
	ChildVids map[string]string
	Kids map[string]*Stub

	LastWriter int

	data []byte

	/* For now, BlockOffsets and BlockLengths are exported fields in the MyNode structure. Can change later if required. */
	BlockOffsets []int
	BlockLengths []int
	DataBlocks []string
}

func (n *MyNode) String() string {
	var data []byte
	if len(n.data) > 10 {
		data = n.data[:10]
	} else {
		data = n.data
	}
	kidsstr := ""
	for _, v := range n.Kids {
		kidsstr += v.String()
	}
	// return fmt.Sprintf("MyNode >>>\ndir=%v\ninode=%d\nVid=%s\nname=%q\nlastwriter=%d\nKids=%v\nDataBlocks=%v\ndata[:10]=%v\n>>>", n.Attrib.Mode.IsDir(), n.Attrib.Inode, n.Vid, n.Name, n.LastWriter, n.Kids, n.DataBlocks, string(data))
	return fmt.Sprintf("Mynode dirty=%v, dir=%v, inode=%d, Vid=%s, name=%q, lastwriter=%d, Kids=%v, DataBlocks=%v, data[:10]=%v, >>>", n.dirty, n.Attrib.Mode.IsDir(), n.Attrib.Inode, n.Vid, n.Name, n.LastWriter, n.Kids, n.DataBlocks, string(data))
}


func (n *MyNode) checkForUpdates() bool {
	updatenode, found := hash2mynode[n.Vid]
	if found {
		n.updateFromNode(updatenode)
		util.P_out("found update to %s!", n.Name)
		delete(hash2mynode, n.Vid)
		return true
	}
	return false
}

func (n *MyNode) updateFromNode(val MyNode) {
	AssertExpanded(n)
	if n.Attrib.Mode.IsDir() {
		n.Vid = val.Vid
		n.Name = val.Name
		n.Attrib = val.Attrib
		n.LastWriter = val.LastWriter
		n.Kids = val.Kids
		n.expanded = false
	} else {
		n.Vid = val.Vid
		n.Name = val.Name
		n.Attrib = val.Attrib
		n.BlockOffsets = val.BlockOffsets
		n.BlockLengths = val.BlockLengths
		n.DataBlocks = val.DataBlocks
		n.LastWriter = val.LastWriter
		n.data = []byte{}
		n.expanded = false
	}
}

func (n *MyNode) WriteBackData() {
	AssertExpanded(n)
	// if im a file, write out my data blocks and hashes
	for i := 0; i < len(n.DataBlocks); i++ {
		str := n.DataBlocks[i]
		off := n.BlockOffsets[i]
		ret := n.BlockLengths[i]
		storage.Put([]byte(fmt.Sprintf("%s:%v", DATA_KEY, str)), n.data[off:off + ret])
	}
}


/* returns type of this mynode */
func (n *MyNode) fuseType() fuse.DirentType {
	if n.Attrib.Mode.IsDir() {
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
	n.Kids = make(map[string]*Stub)

	n.LastWriter = GetMyPid()
}

/* An Attr method to return the basic file attributes defined by Attr. Required to implement Node interface */
func (n *MyNode) Attr() fuse.Attr {
	n.checkForUpdates()
	util.P_out("|%s| attr.Mode = %v", n.Name, n.Attrib)
	return n.Attrib
}

/* checks whether a child with name `name` exists */
func (n *MyNode) Lookup(name string, intr fs.Intr) (fs.Node, fuse.Error) {
	util.P_out("LOOKUP: %s in %s", name, n.Name)
	n.checkForUpdates()
	AssertExpanded(n)
	if k, ok := n.children[name]; ok {
		return k, nil
	}
	return nil, fuse.ENOENT
}

/* reads directory. */
func (n *MyNode) ReadDir(intr fs.Intr) ([]fuse.Dirent, fuse.Error) {
	lock.LOCK.Lock()
	defer lock.LOCK.Unlock()
	n.checkForUpdates()
	AssertExpanded(n)
	util.P_out("performing a readdir on %v", n)
	dirs := make([]fuse.Dirent, 0, 10)
	for k, v := range n.children {
		d := fuse.Dirent{Inode: v.Attrib.Inode, Name: k, Type: v.fuseType()}
		util.P_out("readdir %s => %v", k, d)
		dirs = append(dirs, d)
	}
	return dirs, nil
}

/* must be defined or editing w/ vi or emacs fails. Doesn't have to do anything */
func (n *MyNode) Fsync(req *fuse.FsyncRequest, intr fs.Intr) fuse.Error {
	lock.LOCK.Lock()
	defer lock.LOCK.Unlock()
	return nil
}

/* creates a directory */
func (p *MyNode) Mkdir(req *fuse.MkdirRequest, intr fs.Intr) (fs.Node, fuse.Error) {
	lock.LOCK.Lock()
	defer lock.LOCK.Unlock()
	p.checkForUpdates()
	AssertExpanded(p)

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
			util.P_out("looking through old versions of parent")
			parentVersions := GetNodeVersions(p.NodeID)
			for i := len(parentVersions) - 1; i >= 0; i-- {
				vnode, _ := LoadNodeVersion(parentVersions[i], GetMyPid())
				vid, err := vnode.Kids[filename]
				if err {
					/* version found */
					n, _ = LoadNodeVersion(vid.Vid, GetMyPid())
					break
				}
			}
		} else {
			n = trial
		}

		if(n == nil) {
			return nil, fuse.Errno(syscall.ENOENT)
		}

		if n.Attrib.Mode.IsDir() {
			date := tokens[1]
			finalTime := parseTime(date)
			util.P_out("final time: %v", finalTime)
			versions := GetNodeVersions(n.NodeID)
			var prev *MyNode = nil
			for i := 0; i < len(versions); i++ {
				vnode, _ := LoadNodeVersion(versions[i], GetMyPid())
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
				for k, v := range prev.Kids {
					d.Kids[k] = v
				}
				util.P_out("state of children: %v", d.Kids)
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
				vnode, _ := LoadNodeVersion(versions[i], GetMyPid())
				vnode.Name = vnode.Name + ".[" + vnode.Attrib.Mtime.Format("Mon Jan 2 15:04:05 -0700 MST 2006") + "]"
				vnode.Attrib.Mode = vnode.Attrib.Mode & 0444;	/* make it read-only */
				util.P_out("vnode: %v", vnode.Attrib.Mtime)
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
	lock.LOCK.Lock()
	defer lock.LOCK.Unlock()
	util.P_out("CREATE %s in %s", req.Name, p.Name)
	p.checkForUpdates()
	AssertExpanded(p)
	fmt.Println(req)
	if !isAllowed(p, "w") {
		return nil, nil, fuse.Errno(syscall.EACCES)
	}
	f := new(MyNode)
	f.Init(req.Name, req.Mode, p)
	p.children[req.Name] = f
	updateAncestors(f)
	util.P_out("Create: %s => %v, %v", req.Name, f.Attrib.Mode.Perm(), req.Mode.Perm())
	return f, f, nil
}

/* removes a file */
func (p *MyNode) Remove(req *fuse.RemoveRequest, intr fs.Intr) fuse.Error {
	lock.LOCK.Lock()
	defer lock.LOCK.Unlock()
	p.checkForUpdates()
	AssertExpanded(p)

	if !isAllowed(p, "w") {
		return fuse.Errno(syscall.EACCES)
	}


	//util.P_out("remove: %s", p.Name)
	child, ok := p.children[req.Name] /* child is to be deleted */

	/* update the structure and remove the subtree rooted here */
	if child.archive {
		util.P_out("special case of rmdir: removing archive")
		delete(p.children, req.Name)
		child = nil
		return nil
	}

	if !ok {
		util.P_out("invalid file or directory!")
		return fuse.ENOENT
	} else {
		performDelete := (req.Dir && child.Attrib.Mode.IsDir() && len(child.Kids) == 0) || (!req.Dir && !child.Attrib.Mode.IsDir())

		if performDelete {
			util.P_out("remove successful")
			delete(p.children, req.Name)
			delete(p.Kids, req.Name)
			updateAncestors(p)
		} else {
			util.P_out("kids len = %d", len(child.Kids))
			return fuse.Errno(syscall.EPERM)
		}
	}
	return nil
}

/* write to a file */
func (n *MyNode) Write(req *fuse.WriteRequest, resp *fuse.WriteResponse, intr fs.Intr) fuse.Error {
	lock.LOCK.Lock()
	defer lock.LOCK.Unlock()
	n.checkForUpdates()
	AssertExpanded(n)
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
	n.DataBlocks, n.BlockOffsets, n.BlockLengths = storage.ChunkifyAndStoreRK(n.data)

	/* TODO update ancestors' versions */
	updateAncestors(n)

	return nil
}

/* read from a file */
func (n *MyNode) ReadAll(intr fs.Intr) ([]byte, fuse.Error) {
	lock.LOCK.Lock()
	defer lock.LOCK.Unlock()
	if !isAllowed(n, "r") {
		util.P_out("CANT read stuff from %s (%p)", n.Name, n)
		return nil, fuse.Errno(syscall.EACCES)
	}

	n.checkForUpdates()
	AssertExpanded(n)
	util.P_out("readall on %v (%p)", n, n)
	util.P_out("reading stuff size=%d, from %s", n.Attrib.Size, n.Name)
	util.P_out("datablocks: %v", n.DataBlocks)
	return n.data[:n.Attrib.Size], nil
}

/*
	Flush a file
	This is called on a file whenever that file descriptor is closed
	(There is no guarantee that it will be called after file writes)
*/
func (n *MyNode) Flush(req *fuse.FlushRequest, intr fs.Intr) fuse.Error {
	lock.LOCK.Lock()
	defer lock.LOCK.Unlock()
	return nil
}

/* rename a file (p = parent node) */
func (p *MyNode) Rename(req *fuse.RenameRequest, newDir fs.Node, intr fs.Intr) fuse.Error {
	lock.LOCK.Lock()
	defer lock.LOCK.Unlock()
	p.checkForUpdates()
	AssertExpanded(p)

	if !isAllowed(p, "w") {
		return fuse.Errno(syscall.EACCES)
	}

	/* remove child from current parent */
	childToRename := p.children[req.OldName]

	if childToRename.archive {
		return fuse.Errno(syscall.EPERM)
	}

	delete(p.children, req.OldName)
	delete(p.Kids, req.OldName)
	updateAncestors(p)

	/* attach to new parent, passed newDir better be a *MyNode */
	newParent, ok := newDir.(*MyNode)
	AssertExpanded(newParent)
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
	n.checkForUpdates()
	AssertExpanded(n)
	//util.P_out("getting attr for %s", n.Name)
	resp.Attr = n.Attrib
	return nil
}

/* implementing this otherwise can't set permissions */
func (n *MyNode) Setattr(req *fuse.SetattrRequest, resp *fuse.SetattrResponse, intr fs.Intr) fuse.Error {
	lock.LOCK.Lock()
	defer lock.LOCK.Unlock()
	n.checkForUpdates()
	AssertExpanded(n)

	if !isAllowed(n, "w") {
		util.P_out(">>>>>>>>>>>>>>>>>>>>>>>>>>>> set attr is not allowed for %s", n.Name)
		return fuse.Errno(syscall.EACCES)
	}

	if req.Valid.Mode() {
		util.P_out("================================ %s => setting mode perm: %v", n.Name, req.Mode.Perm())
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
