#include <stdint.h>
#include <stdio.h>

#include "kvdblite.h"

static unsigned long rand_X = 123456789;
unsigned long VAX_rng(void) { return (rand_X = 69069 * rand_X + 362437); }

int main() {
  //struct avltree *avl = avl_make("mykvdb.kvb");
  struct avltree *avl = avl_make(NULL);
  printf("size: %d\n", avl_db_size(avl));
  printf("valid: %d\n", avl_check_valid(avl));

  if (avl_db_size(avl) == 0) {
#define TREESIZE 100000
    struct node *result = NULL;

    uint8_t bk[64];
    uint8_t bv[64];

    for (int i = 0; i < TREESIZE; i++) {
      sprintf(bk, "%d", (int)VAX_rng() & 0xFFFFFFFF);
      sprintf(bv, "%lu", VAX_rng());
      avl_insert(avl, bk, bv);
      if(i%500==0)
        printf("%d\n", i);
    }

    // if (avl_save_database(avl) < 0) {
    //   printf("Failed to save database to disk\n");
    // }

    // Post save operations that will go in transaction file
    avl_insert(avl, "1", "11111");
    avl_insert(avl, "2", "2222");
    avl_insert(avl, "3", "333");
    avl_remove(avl, "1");
  }

  struct avl_lookup_result *r = avl_lookup(avl, "1");
  if (r == NULL) {
    printf("1 doesn't exist in DB.. That is good!\n");
  } else {
    printf("1 exists in DB.. That is BAD!\n");
  }

  r = avl_lookup(avl, "3");
  if (r == NULL) {
    printf("3 doesn't exist in DB.. That is bad!\n");
  } else {
    printf("3 exists in DB.. That is good!\n");
  }

  // printf("valid: %d\n", avl_check_valid(avl));
  //avl_debug_inorder(avl);

  //   rand_X = 123456789;
  //   for (int i = 0; i < TREESIZE; i++) {
  //     sprintf(bk, "%d", (int)VAX_rng() & 0xFFFF);
  //     sprintf(bv, "%lu", VAX_rng());
  //     avl_remove(avl, bk);
  //   }
  //   printf("valid: %d\n", avl_check_valid(avl));
  //   avl_debug_inorder(avl);
}