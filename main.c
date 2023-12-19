/*
 * Copyright (C) 2023 Gary Sims
 * All rights reserved.
 *
 * SPDX-License-Identifier: BSD-3-Clause
 */
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#include "kvdblite.h"

//
// Compile with:
// gcc -o main main.c kvdblite.c
//

// Test program to create key-value DB, populate it, save it to disk
// and then add three more keys, and remove one.
// Finally check the validity of the database (size etc)
// Prints "PASSED" if everything is OK
// Run it a second time to load the DB from the disk rather than
// populate and empty DB.

#define TREESIZE 500

static unsigned long rand_X = 123456789;
unsigned long VAX_rng(void) { return (rand_X = 69069 * rand_X + 362437); }

int main() {
  struct avltree *avl = avl_make("mykvdb.kvb");

  if (avl_db_size(avl) == 0) {
    // Empty DB, fill it!
    struct node *result = NULL;

    uint8_t bk[64];
    uint8_t bv[64];

    for (int i = 0; i < TREESIZE; i++) {
      sprintf(bk, "%u", (unsigned int)VAX_rng() & 0xFFFFFFFF);
      sprintf(bv, "%lu", VAX_rng());
      avl_insert(avl, bk, bv);
    }

    if (avl_save_database(avl) < 0) {
      printf("FAIL: Failed to save database to disk\n");
      exit(-1);
    }

    // Post save operations that will go in transaction file
    avl_insert(avl, "1", "11111");
    avl_insert(avl, "2", "22222");
    avl_insert(avl, "3", "33333");
    avl_remove(avl, "1");
  }

  struct avl_lookup_result *r = avl_lookup(avl, "1");
  if (r != NULL) {
    printf("FAIL: 1 exists in DB.. That is BAD!\n");
    exit(-1);
  }

  r = avl_lookup(avl, "3");
  if (r == NULL) {
    printf("FAIL: 3 doesn't exist in DB.. That is bad!\n");
    exit(-1);
  }
  if (avl_db_size(avl) != TREESIZE + 2) {
    printf("FAIL: Tree is wrong size\n");
    exit(-1);
  }

  if (avl_check_valid(avl) < 0) {
    printf("FAIL: Tree is not internally valid\n");
    exit(-1);
  }

  printf("PASSED\n");
}
