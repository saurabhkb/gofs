package main

import (
	"time"
	"os"
	"fmt"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
)


var uid = os.Geteuid()
var gid = os.Getegid()

/*
*	MyNode
*	MyNode implements the Node interface (as it has a Attr() method which returns an object implementing Attr)
*/
type MyNode struct {
	name		string
	attr		fuse.Attr
	kids		map[string]*MyNode
	data		[]byte
}

func (n *MyNode) String() string {
		return fmt.Sprintf("MyNode inode=%d, name=%q", n.Attr().Inode, n.name)
}

/* whether this mynode is a directory */
func (n *MyNode) isDir() bool {
	//return (n.attr.Mode & os.ModeDir) != 0
	return n.attr.Mode.IsDir()
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
func (n *MyNode) Init(name string, mode os.FileMode) {

	x := GetAvailableInode()
	n.attr.Inode = x
	n.attr.Nlink = 1
	n.name = name

	tm := time.Now()
	n.attr.Atime = tm
	n.attr.Mtime = tm
	n.attr.Ctime = tm
	n.attr.Crtime = tm
	n.attr.Mode = mode

	n.attr.Gid = uint32(gid)
	n.attr.Uid = uint32(uid)

	n.attr.Size = 0

	n.kids = make(map[string]*MyNode)

	P_out("INIT: inited node inode %d, %q and len = %d\n", n.attr.Inode, name, len(n.kids))
	P_out("INIT: number of kids for %q is %d\n", name, len(n.kids))

	// register node in the inode-*mynode map
	RegisterNode(n.Attr().Inode, n)
}

/* An Attr method to return the basic file attributes defined by Attr. Required to implement Node interface */
func (n *MyNode) Attr() fuse.Attr {
	return n.attr
}

/* checks whether a child with name `name` exists */
func (n *MyNode) Lookup(name string, intr fs.Intr) (fs.Node, fuse.Error) {
	P_out("LOOKUP: looking up %v in %v", name, n)
	if k, ok := n.kids[name]; ok {
		return k, nil
	}
	return nil, fuse.ENOENT
}

/* reads directory. request is made  */
func (n *MyNode) ReadDir(intr fs.Intr) ([]fuse.Dirent, fuse.Error) {
	dirs := make([]fuse.Dirent, 0, 10)
	for k,v:= range(n.kids) {
		dirs = append(dirs, fuse.Dirent{Inode: v.attr.Inode, Name: k, Type: v.fuseType()})
	}
	return dirs, nil
}


/* must be defined or editing w/ vi or emacs fails. Doesn't have to do anything */
func (n *MyNode)Fsync(req *fuse.FsyncRequest, intr fs.Intr) fuse.Error {
	fmt.Println("FSYNC: ", req)
	return nil
}

/* creates a directory */
func (p *MyNode) Mkdir(req *fuse.MkdirRequest, intr fs.Intr) (fs.Node, fuse.Error) {
	P_out("MKDIR: %q in %q\n", req.Name, p.name)
	d := new(MyNode)
	d.Init(req.Name, os.ModeDir | 0755)
	p.kids[req.Name] = d
	return d, nil
}

/* creates a file */
func (p *MyNode) Create(req *fuse.CreateRequest, resp *fuse.CreateResponse, intr fs.Intr) (fs.Node, fs.Handle, fuse.Error) {
	P_out("CREATE: %q in %q\n with mode %v", req.Name, p.name, req.Mode)
	P_out("CREATE: response: %v", resp)
	f := new(MyNode)
	f.Init(req.Name, req.Mode)
	p.kids[req.Name] = f
	// nothing specified in the Handle interface, so just return same mynode object. mynode has method ReadAll and hence implements 
	// interface HandleReadAller
	return f, f, nil
}

/* removes a file. this function looks to be called on the parent directory of the unfortunate file */
func (p *MyNode) Remove(req *fuse.RemoveRequest, intr fs.Intr) fuse.Error {
	P_out("REMOVE: try %q from %q\n", req.Name, p.name)
	child, ok := p.kids[req.Name]	// child is to be deleted
	if !ok {
		P_out("invalid file or directory!")
		return fuse.ENOENT
	} else {
		if req.Dir {
			// this is a rmdir
			if child.isDir() {
				// delete directory only if it is empty
				if len(child.kids) == 0 {
					P_out("removing folder %q in %q\n", req.Name, p.name)
					UnregisterNode(child.Attr().Inode)
					delete(p.kids, req.Name)
					child = nil
				} else {
					P_out("cannot remove folder with %d children", len(child.kids))
					return fuse.EPERM
				}
			} else {
				P_out("cannot rmdir a file!")
				return fuse.EPERM
			}
		} else {
			// this is an rm
			if child.isDir() {
				P_out("REMOVE: cant remove directory %q in %q\n", req.Name, p.name)
				return fuse.EPERM
			}
			P_out("REMOVE: removing file %q in %q\n", child.name, p.name)
			UnregisterNode(child.Attr().Inode)
			delete(p.kids, req.Name)
			child = nil
		}
	}
	return nil
}

/* write to a file */
func (n *MyNode) Write(req *fuse.WriteRequest, resp *fuse.WriteResponse, intr fs.Intr) fuse.Error {
	// check permissions and file open mode?
	// update attributes? (last write time, etc.)

	// for now, we assume that this method never fails
	// should fail only if permissions error or if there isnt enough memory...

	fmt.Printf("WRITE: header=%s, handle=%#x, len=%d, offset=%d, fl=%v\n", req.Header, req.Handle, len(req.Data), req.Offset, req.Flags)

	// helper variable to avoid all the typecasts
	dataLen64 := int64(len(req.Data))

	// extend if necessary
	if int64(cap(n.data)) < req.Offset + dataLen64 {
		t := make([]byte, req.Offset + dataLen64)
		copy(t, n.data)
		n.data = t
	}

	// copy out the data
	for i := 0; i < len(req.Data); i++ {
		n.data[int(req.Offset) + i] = req.Data[i]
	}

	// crucial, otherwise fuse thinks that data is still 0B
	n.attr.Size = uint64(len(n.data))

	// how many bytes were written out (crucial, otherwise applications think that nothing was written out and get pissed)
	resp.Size = len(req.Data)

	// update modified time
	n.attr.Mtime = time.Now()

	return nil;
}

/* read from a file */
func (n *MyNode) ReadAll(intr fs.Intr) ([]byte, fuse.Error) {
	return n.data, nil
}

/*
Flush a file
This is called on a file whenever that file descriptor is closed
(There is no guarantee that it will be called after file writes)
Hence, when copying a file out of the filesystem, the file is opened, its contents read, a new file created, the contents written
and both files closed.
On closing, the original file is flushed.


*** Based on standard implementations, this function does not flush file data out to disk.
It simply flushes the user level buffers (buffers maintained by the C library via FILE, etc.) out
But there is yet another level of caching done by the kernel so it is possible that the user space changes are simply flushed out to the kernel caches
Calling Fsync works at one level lower: it flushes the kernel's buffers out to permanent storage
But there is (possibly) yet another level of caching done by the actual hardware (eg. hard disk) so it is possible that the changes are simply flushed out to the hardware's buffers
There is no guarantee that the modified data has actually hit the platters.
For that, there appears to be something called FUA (Force Unit Access) which is an IO write command option that actually provides a guarantee that
data will be written out to permanent storage (the actual platters) whatever the buffering is at any level
*/
func (n *MyNode) Flush(req *fuse.FlushRequest, intr fs.Intr) fuse.Error {
	// might want to write out into a file here
	P_out("FLUSH: %q\n", n.name)
	return nil
}

/* rename a file (p = parent node) */
func (p *MyNode) Rename(req *fuse.RenameRequest, newDir fs.Node, intr fs.Intr) fuse.Error {
	// newDir is the new path of the file/directory
	// req.OldName = old name req.NewName = new name

	fmt.Printf("RENAME: newDirNid=%v, newDir=[%v], oldname=%s, newname=%s\n", req.NewDir, newDir, req.OldName, req.NewName)
	fmt.Println("RENAME: this node: ", p)

	x := newDir.(*MyNode)
	fmt.Println("RENAME: x:", x)

	// remove child from current parent
	childToRename := p.kids[req.OldName]
	delete(p.kids, req.OldName)

	// attach to new parent, passed newDir better be a *MyNode
	newParent := newDir.(*MyNode)
	fmt.Println("RENAME: new parent mynode: ", newParent)
	newParent.kids[req.NewName] = childToRename

	return nil
}

/* get file attributes */
func (n *MyNode) Getattr(req *fuse.GetattrRequest, resp *fuse.GetattrResponse, intr fs.Intr) fuse.Error {
	resp.Attr = n.attr
	// ignoring any possible error for the time being
	// resp.AttrValid, _ = time.ParseDuration("1h")
	return nil
}

/* implementing this otherwise can't make an executable. gcc attempts to call this to assign execute permissions to a.out */
func (n *MyNode) Setattr(req *fuse.SetattrRequest, resp *fuse.SetattrResponse, intr fs.Intr) fuse.Error {
	P_out("SETATTR: %v", req)
	if req.Valid.Mode() {
		n.attr.Mode = req.Mode
	}
	if req.Valid.Atime() {
		n.attr.Atime = req.Atime
	}
	if req.Valid.Mtime() {
		n.attr.Mtime = req.Mtime
	}
	if req.Valid.Gid() {
		n.attr.Gid = req.Gid
	}
	if req.Valid.Uid() {
		n.attr.Uid = req.Uid
	}
	return nil
}

// func (n *MyNode) Link(req *fuse.LinkRequest, node fs.Node, intr fs.Intr) (fs.Node, fuse.Error) {
// 	P_out("LINK: req=%v, node=%v", req, node)
// 	return nil, nil
// }
