package main

/*
=======================
DATABASE INTERFACE
=======================
*/

import (
	"github.com/syndtr/goleveldb/leveldb"
)

type MyStore struct {
	db *leveldb.DB
}

func (d *MyStore) Init(dbpath string) {
	d.db, _ = leveldb.OpenFile(dbpath, nil)
}

func (d *MyStore) Get(key []byte) ([]byte, error) {
	return d.db.Get(key, nil)
}

func (d *MyStore) Put(key []byte, val []byte) error {
	return d.db.Put(key, val, nil)
}

func (d *MyStore) Close() error {
	return d.db.Close()
}
