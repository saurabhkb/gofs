import leveldb
import sys
import re

''' easy interface to leveldb '''

db = leveldb.LevelDB("/tmp/db/alice")
opt = sys.argv[1]

if opt == "-h":
	print "kc = key count\nlk = list keys\ndel = clear db\nget = get value\ngetr = get keys matching regex"

# list key count
if opt == "kc":
	print len(list(db.RangeIter()))

# list keys
if opt == "lk":
	for k, v in db.RangeIter():
		print k
	print "=== OK ==="

# delete all keys
if opt == "del":
	for k, v in db.RangeIter():
		db.Delete(k)
	print "=== OK ==="

# get value for key
if opt == "get":
	key = sys.argv[2]
	print db.Get(key)
	print "=== OK ==="

# get values for keys matching regex
if opt == "getr":
	key = sys.argv[2]
	for k, v in db.RangeIter():
		if re.match(key, k):
			print k
	print "=== OK ==="
