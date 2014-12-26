package main

/*
*	black box like interface which provides inode numbers and stores mappings
*/

var nodeMap = make(map[uint64]*MyNode)

/*
inode number for the file/dir. might want to maintain some inode list + free inode list to allow for inode number reuse
 */
var nextInode uint64


/* self explanatory */
func GetAvailableInode() uint64 {
	nextInode++
	return nextInode
}

/* update inode number to mynode reference map */
func RegisterNode(inode uint64, node *MyNode) {
	nodeMap[inode] = node
}

/* release inode number */
func UnregisterNode(inode uint64) {
	delete(nodeMap, inode)
}

/* get mynode reference given an inode number */
func GetNodeReference(inode uint64) *MyNode {
	return nodeMap[inode]
}
