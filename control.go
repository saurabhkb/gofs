package main

import (
	"fmt"
	"encoding/json"
	"os"
	"strconv"
	"sync"
)

/* namespaces for the data store */
const DATA_KEY string = "DATA_KEY"
const NODE_VERSION_KEY string = "NDV"
const NEXT_AVAILABLE_VID_KEY string = "NXT_AVLBL_VID"
const NEXT_AVAILABLE_INODE_KEY string = "NXT_AVLBL_INODE"
const STATE_KEY = "STATE"


/* a structure to store state */
type STATE struct {
	Root_version_bootstrap int	/* latest root version */
	NextInode uint64			/* next available inode number */
	NextVId int					/* next available version number */
}

/* important variables */
var db *MyStore
var state STATE
var LOCK *sync.Mutex

func InitDatabase(dbpath string) {
	db = &MyStore{}
	db.Init(dbpath)
}

func LoadState() {
	statestr, _ := db.Get([]byte(STATE_KEY))
	json.Unmarshal(statestr, &state)
	state.NextInode++
	P_out("state: %v", state)
}

/* places an fs object in the memory pointed to by the argument */
func LoadFS(fs *MyFS) {
	/* state.Root_version_bootstrap is Vid of root of filesystem */
	str := fmt.Sprintf("%s:%d", NODE_VERSION_KEY, state.Root_version_bootstrap)
	P_out("%v\n", str)
	rootdirstr, err := db.Get([]byte(str))
	if err != nil {
		/* key most likely doesn't exist */
		P_out("%v", err)
		fs.RootDir = new(MyNode)
		fs.RootDir.Init("/", os.ModeDir | 0755, nil)
		SaveNodeVersion(fs.RootDir)
	} else {
		json.Unmarshal(rootdirstr, &fs.RootDir)
	}
	assertExpanded(fs.RootDir)
}

/* initialize the lock */
func InitLock() {
	LOCK = &sync.Mutex{}
}

/* self explanatory */
func GetAvailableInode() uint64 {
	state.NextInode++
	return state.NextInode
}


/* allocate globally unique version numbers */
func GetAvailableVersionId() int {
	state.NextVId++
	return state.NextVId
}


/* method which takes data and chunks it for storage based on Rabin Karp. returns an array of chunk hashes, offsets and lengths */
func ChunkifyAndStoreRK(data []byte) ([]string, []int, []int) {
	off := 0
	chunkHashes := []string{}
	offsets := []int{}
	lengths := []int{}
	dataLen := len(data)
	for off < len(data) {
		ret, hash := rkchunk(data[off:], uint64(dataLen - off))
		chunkHashes = append(chunkHashes, strconv.FormatUint(hash, 10))
		offsets = append(offsets, off)
		lengths = append(lengths, int(ret))
		off += int(ret)
	}
	return chunkHashes, offsets, lengths
}


/* "saves" a node corresponding to a new version -> basically sets its dirty flag */
func SaveNodeVersion(node *MyNode) bool {
	if node.parent == nil { /* root. update the bootstrap value */
		state.Root_version_bootstrap = node.Vid
	}
	node.dirty = true
	return true
}

/* load node */
func loadNodeVersion(Vid int) *MyNode {
	var x MyNode
	nodestr, _ := db.Get([]byte(fmt.Sprintf("%s:%v", NODE_VERSION_KEY, Vid)))
	json.Unmarshal(nodestr, &x)
	return &x
}

/* load data */
func loadDataChunk(hash string) []byte {
	ret, _ := db.Get([]byte(fmt.Sprintf("%s:%v", DATA_KEY, hash)))
	return ret
}


/* expands this node and loads its children (if it hasn't already been done) */
func assertExpanded(node *MyNode) {
	if !node.expanded {
		P_out("expanding: %v", node.Name)
		/* dirty, children, data */
		node.dirty = false;

		if(node.isDir()) {
			/* children, not data */
			node.children = make(map[string]*MyNode)
			for name, Vid := range node.ChildVids {
				node.children[name] = loadNodeVersion(Vid)
				node.children[name].parent = node
			}
		} else {
			/* data, not children */
			for i := 0; i < len(node.DataBlocks); i++ {
				loadedData := loadDataChunk(node.DataBlocks[i])
				for j := 0; j < len(loadedData); j++ {
					node.data = append(node.data, loadedData[j])
				}
			}
		}

		node.expanded = true
	} else {
		P_out("node %v is already expanded. twiddling thumbs...", node.Name)
	}
}
