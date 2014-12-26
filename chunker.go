package main

import (
	"encoding/hex"
	"crypto/sha1"
)

/*
=======================
RABIN KARP CHUNKING
=======================
*/

const HASHLEN uint64 = 32
const THE_PRIME uint64 = 31
const MINCHUNK uint64 = 2048
const TARGETCHUNK uint64 = 4096
const MAXCHUNK uint64 = 8192


var b uint64
var b_n uint64
var saved[256] uint64

func rkchunk(buf []uint8, l uint64) uint64 {
	var i uint64
	var hash uint64 = 0
	var off uint64 = 0

	if b == 0 {
		b = THE_PRIME

		b_n = 1
		for i = 0; i < HASHLEN - 1; i++ {
			b_n *= b
		}

		for i = 0; i < 256; i++ {
			saved[i] = i * b_n
		}
	}

	for off = 0; off < HASHLEN && off < l; off++ {
		hash = hash * b + uint64(buf[off])
	}

	for(off < l) {
		hash = (hash - saved[buf[off - HASHLEN]]) * b + uint64(buf[off])
		off++

		if (off >= MINCHUNK && hash % TARGETCHUNK == 1) || (off >= MAXCHUNK) {
			return off
		}
	}
	return off
}



/* method which takes data and chunks it for storage based on Rabin Karp. returns an array of chunk hashes, offsets and lengths */
func ChunkifyAndStoreRK(data []byte) ([]string, []int, []int) {
	off := 0
	chunkHashes := []string{}
	offsets := []int{}
	lengths := []int{}
	dataLen := len(data)
	for off < len(data) {
		ret := rkchunk(data[off:], uint64(dataLen - off))
		hash := sha1.Sum(data[off : off + int(ret)])
		hashStr := hex.EncodeToString(hash[:])
		chunkHashes = append(chunkHashes, hashStr)
		offsets = append(offsets, off)
		lengths = append(lengths, int(ret))
		off += int(ret)
	}
	return chunkHashes, offsets, lengths
}

