#include <sys/types.h>
#include <sys/stat.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <err.h>

#include "btree.h"

#define DBPATH	"/tmp/foo"

int
main(void)
{
	struct btree *bt;
	struct btree_txn *txn;
	struct btval key, val;
	struct stat st;

	if ((bt = btree_open(DBPATH, 0, 0600)) == NULL)
		err(1, "btree_open");

	if (btree_sync(bt) != BT_SUCCESS)
		err(1, "btree_sync");

	sync();
	sleep(1);
	sync();
	sleep(1);

	if (stat(DBPATH, &st) < 0)
		err(1, "stat");
	printf("size 1 = %llu (%lluK)\n", st.st_size, st.st_size / 1024);

	if ((txn = btree_txn_begin(bt, 0)) == NULL)
		err(1, "btree_txn_begin");

	memset(&key, 0, sizeof(key));
	key.data = "Hi";
	key.size = 3;

	memset(&val, 0, sizeof(val));
	val.data = "Mike";
	val.size = 5;

	if (btree_txn_put(bt, txn, &key, &val, 0) != BT_SUCCESS)
		err(1, "btree_txn_put");
	if (btree_txn_commit(txn) != BT_SUCCESS)
		err(1, "btree_txn_commit");

	sync();
	sleep(1);
	sync();
	sleep(1);

	if (stat(DBPATH, &st) < 0)
		err(1, "stat");
	printf("size 2 = %llu (%lluK)\n", st.st_size, st.st_size / 1024);

	if (btree_compact(bt) != BT_SUCCESS)
		err(1, "btree_compact");

	sync();
	sleep(1);
	sync();
	sleep(1);

	if (stat(DBPATH, &st) < 0)
		err(1, "stat");
	printf("size 3 = %llu (%lluK)\n", st.st_size, st.st_size / 1024);

	return 0;
}
