#include <stdio.h>
#include <stdint.h>

#include "kvdblite.h"

static unsigned long rand_X = 123456789;
unsigned long VAX_rng(void) { return (rand_X = 69069 * rand_X + 362437); }

int main() {
  struct avltree *avl = avl_make("mykvdb.kvb");
  printf("size: %d\n", avl_db_size(avl));
  printf("valid: %d\n", avl_check_valid(avl));

  if (avl_db_size(avl) == 0) {
#define TREESIZE 12
    struct node *result = NULL;

    uint8_t bk[64];
    uint8_t bv[64];

    for (int i = 0; i < TREESIZE; i++) {
      sprintf(bk, "%d", (int)VAX_rng() & 0xFFFF);
      sprintf(bv, "%lu", VAX_rng());
      avl_insert(avl, bk, bv);
    }

    if (avl_save_database(avl) < 0) {
      printf("Failed to save database to disk\n");
    }
  }
  // printf("valid: %d\n", avl_check_valid(avl));
  // avl_debug_inorder(avl);

  //   rand_X = 123456789;
  //   for (int i = 0; i < TREESIZE; i++) {
  //     sprintf(bk, "%d", (int)VAX_rng() & 0xFFFF);
  //     sprintf(bv, "%lu", VAX_rng());
  //     avl_remove(avl, bk);
  //   }
  //   printf("valid: %d\n", avl_check_valid(avl));
  //   avl_debug_inorder(avl);
}