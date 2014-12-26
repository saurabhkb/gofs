package storage

/*
=======================
DATABASE INTERFACE
=======================
*/

import (
	"os"
	"fmt"
)

import "github.com/syndtr/goleveldb/leveldb"

var db *leveldb.DB
var path string

func Init(dbpath string) {
	path = dbpath
	db, _ = leveldb.OpenFile(dbpath, nil)
}

func Get(key []byte) ([]byte, error) {
	return db.Get(key, nil)
}

func Put(key []byte, val []byte) error {
	return db.Put(key, val, nil)
}

func Clear() {
	fmt.Println("clearing path: ", path)
	Close()
	os.RemoveAll(path)
	Init(path)
}

func Close() error {
	return db.Close()
}
