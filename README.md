# KVDBlite

A simple key-value database with journaling.

## Details

This is a key-value database, where you store a key and a value for that key, like:
```score: 1234
username: gary
8586022215377: {"product": "Multivitamins", "price”: 4.99}
f32132c6-5604-4c55-9aca-b76c15ce4ecc: true
```

kvdblite keeps the data in RAM at all times and saves the data to the disk so that it can be reloaded at restart. It is written in C.

The data is stored using a balanced binary tree (and AVL tree).

Compile the example program in main.c like this:
```
gcc -o main main.c kvdblite.c
```
The example program (main.c) creates key-value DB, populates it, saves it to disk and then adds three more keys, and removes one. Finally, it checks the validity of the database (size etc). Prints "PASSED" if everything is OK.

Run it a second time to load the DB from the disk rather than populate an empty DB.

## AVL Tree
- An AVL tree (named after inventors Adelson-Velsky and Landis) is a self-balancing binary search tree.
- In an AVL tree, the heights of the two child subtrees of any node differ by at most one; if at any time they differ by more than one, rebalancing is done to restore this property. 
- Georgy Adelson-Velsky and Evgenii Landis, published "An algorithm for the organization of information" paper in 1962 paper 
- It is the oldest self-balancing binary search tree data structure to be invented.

## Writing data to disk
There are several ways we could write the data to disk. More than I mention here. But here are some examples:

### Write the whole database in a human-readable format like JSON
- Good for debugging
- Can be hand-edited
- Good for copying data from one DB to another
### Write the whole database in a binary format
- Faster
- Can include additional error-checking info inline (CRC etc)

### Performance
But the problem with writing the whole database is that after every action on the tree, all the data needs to be written. If I add 1000 items to the database then 1000 times the whole database needs to be written to disk.

Writing out the whole database will hamper performance. It will become disk IO limited.

### Resilience

Also, if there is a crash while writing the WHOLE database to the disk then the db will be corrupt, probably everything is lost.

One way to improve resilience is to keep the old copy until the new version has been written out. Assume that rename is an atomic operation.

### Transactions
One method to improve the performance while offering resilience is to use transactions/journaling.

Here every operation is recorded in a journal.
- Faster because only need to append to journal
- Faster because only writing out the data associated with one transaction

At startup the existing db is loaded, and then the transactions are replayed to bring the db up to date

API call to save DB in current state and empty the journal

## Potential backup/recovery techniques
Note: Not implemented yet

When saving the whole database:
- Copy existing database and existing journal file to a backup (replace previous backup if needed)
- Save database to temporary file
- Remain temporary file to main database name
- Truncate journal file

Assuming that there has been some corruption and there are no backup files
- Rudimentary ability to parse database file looking for magic numbers, read key and value, check CRC and build database.

## TODO
- Performance improvements
- Better error handling
- Import and export database to JSON
- Use network order for cross platform compatibility
- Way to turn temporarily turn off journaling
- Memory protection mprotect()for avltree struct
- Thread safe (locking etc)
- Write a demo web server that is compatible with various key-store REST APIs






