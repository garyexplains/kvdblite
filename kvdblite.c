/*
 * Copyright (C) 2023 Gary Sims
 * All rights reserved.
 *
 * SPDX-License-Identifier: BSD-3-Clause
 */

#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "kvdblite.h"

#define KVDBLITE_OP_INSERT 43
#define KVDBLITE_OP_REMOVE 45

// TODO
// Increase performance of adding 100,000 entries. Seems creating journal file is slow.
// Error handling needs to be robust and consistent
// avl_import(char * fn) and avl_append(char *fn). The import needs to check that root is NULL.
// Memory protection using mprotect() for struct avltree, 
// Make thread safe (mutex locking etc)
// Use network order when writing binary int32/uint32 etc, for cross platform compatibility

struct node {
  struct node *left, *right;
  int diff;
  avl_key_t *key;
  avl_value_t *value;
};

struct avltree {
  struct node *root;
  uint8_t *dbname;
  uint8_t *journalname;
};

// Forwards
static int insert(avl_key_t *key, avl_value_t *value, struct node **rp);
static int remove_root(struct node **rp);
static int remove_(avl_key_t *key, struct node **rp);
static int truncate_transaction_file(struct avltree *avl);

//
// CRC32
//

// Precomputed table for CRC32 checksums
uint32_t crc32_table[256];

// Initialize the precomputed table
void generate_CRC32_table() {
    uint32_t c;
    for (int i = 0; i < 256; i++) {
        c = i;
        for (int j = 0; j < 8; j++) {
            if (c & 1) {
                c = 0xEDB88320L ^ (c >> 1);
            } else {
                c = c >> 1;
            }
        }
        crc32_table[i] = c;
    }
}

// Compute the CRC32 checksum
uint32_t calc_CRC32(unsigned char *buf, int len, uint32_t prev_crc) {
    uint32_t crc = ~prev_crc;
    while (len--) {
        crc = crc32_table[(crc ^ *buf++) & 0xFF] ^ (crc >> 8);
    }
    return crc ^ 0xFFFFFFFF;
}

// Wrapper function for calculating the CRC32 for two concatenated memory blocks
uint32_t key_and_value_CRC32(avl_key_t *k, int klen, avl_value_t *v, int vlen) {
    uint32_t crc = 0; // Initial CRC value
    crc = calc_CRC32(k, klen, crc);
    crc = calc_CRC32(v, vlen, crc);
    return crc;
}

//
// END CRC32
//

//
// DISK IO
//

static int fwrite_str(uint8_t *s, FILE *file) {
  size_t itemsWritten = fwrite(s, strlen(s), 1, file);
  if (itemsWritten != 1) {
    return -1;
  }
  return 1;
}
static int fwrite_uint32_t(uint32_t value, FILE *file) {
  // Need to use network order for cross platform compatibility
  size_t itemsWritten = fwrite(&value, sizeof(uint32_t), 1, file);
  if (itemsWritten != 1) {
    return -11;
  }
  return 1;
}

static int fwrite_uint8_t(uint8_t value, FILE *file) {
  size_t itemsWritten = fwrite(&value, sizeof(uint8_t), 1, file);
  if (itemsWritten != 1) {
    return -11;
  }
  return 1;
}

static int fread_str(uint8_t **v, uint32_t l, FILE *file) {
  // TODO: Maybe sanity check the string length
  if(l==0)
    return -1;
  *v = malloc(l + 1);
  size_t itemsRead = fread(*v, l, 1, file);
  if (itemsRead != 1) {
    free(*v);
    *v = NULL;
    return -1;
  }
  (*v)[l] = 0;

  return 1;
}

static int fread_uint32_t(uint32_t *value, FILE *file) {
  size_t itemsRead = fread(value, sizeof(uint32_t), 1, file);

  if (itemsRead != 1) {
    return -1;
  }
  return 1;
}

static int fread_uint8_t(uint8_t *value, FILE *file) {
  size_t itemsRead = fread(value, sizeof(uint8_t), 1, file);

  if (itemsRead != 1) {
    return -1;
  }

  return 1;
}

static void save_tree_to_disk(struct node *root, FILE *file) {
  uint32_t l;

  // First save magic number
  uint32_t magic = 0x42473000;
  fwrite_uint32_t(magic, file);

  if (root == NULL) {
    l = 0;
    fwrite_uint32_t(l, file);
    return;
  }

  // Save current node's key and value
  l = strlen(root->key);
  fwrite_uint32_t(l, file);
  fwrite_str(root->key, file);

  l = strlen(root->value);
  fwrite_uint32_t(l, file);
  fwrite_str(root->value, file);

  // Save CRC32 of key and value concatenated
  fwrite_uint32_t(key_and_value_CRC32(root->key, strlen(root->key), root->value,
                                      strlen(root->value)),
                  file);

  // Save diff value
  fwrite_uint32_t((uint32_t) root->diff, file);

  // Save left subtree
  save_tree_to_disk(root->left, file);

  // Save right subtree
  save_tree_to_disk(root->right, file);
}

int avl_save_database(struct avltree *avl) {
  if(avl->dbname==NULL) {
    return KVDBLITE_DBNAME_IS_NULL;
  }

  FILE *file = fopen(avl->dbname, "wb");
  if (!file) {
    return KVDBLITE_FAILED_TO_OPEN_DB_FILE;
  }

  save_tree_to_disk(avl->root, file);

  fclose(file);
  // If crash happens here, after fclose but before transaction file is truncated
  // then at reload the transactions will be applied aghain, but they will be duplicates
  // and have no affect on the final state of the database
  truncate_transaction_file(avl);
  return KVDBLITE_SUCCESS;
}

static struct node *load_tree_from_disk(FILE *file) {
  //char key[256], value[256];
  uint32_t l, crc_calculated, crc_from_file;
  uint8_t *v;
  
  // Magic number (0x42473000)
  if (fread_uint32_t(&l, file) < 0)
    return NULL;
  if (l == 0)
    return NULL;
  if (l!=0x42473000) {
    // Bad magic number
    return NULL;
  }

  // Read the key length
  if (fread_uint32_t(&l, file) < 0)
    return NULL;
  if (l == 0) {
    // A length of zero here also marks the end of a branch
    return NULL;
  }

  // Read the key
  fread_str(&v, l, file);

  struct node *new_node = malloc(sizeof *new_node);
  if (!new_node) {
    perror("Failed to allocate memory for node");
    exit(EXIT_FAILURE);
  }
  new_node->key = strdup(v);
  free(v);

  // Read the value length
  if (fread_uint32_t(&l, file) < 0)
    return NULL;
  // Read the value
  fread_str(&v, l, file);
  new_node->value = strdup(v);
  free(v);

  // Read the CRC32 and check
  if (fread_uint32_t(&crc_from_file, file) < 0)
    return NULL;
  crc_calculated = key_and_value_CRC32(new_node->key, strlen(new_node->key), new_node->value, strlen(new_node->value));
  if (crc_calculated!=crc_from_file) {
    // CRC error
    return NULL;
  }

  // Load diff value
  if (fread_uint32_t(&new_node->diff, file) < 0)
    return NULL;

  new_node->left = load_tree_from_disk(file);
  new_node->right = load_tree_from_disk(file);

  return new_node;
}

static int load_avl_tree(struct avltree *avl, const char *filename) {
  FILE *file = fopen(filename, "r");
  if (!file) {
    return KVDBLITE_FAILED_TO_OPEN_DB_FILE;
  }

  avl->root = load_tree_from_disk(file);

  fclose(file);
  return 0;
}
//
// END DISK
//

//
// Journalling
//

static int truncate_transaction_file(struct avltree *avl) {
  FILE *file = fopen(avl->journalname, "wb");
  if (!file) {
    return KVDBLITE_FAILED_TO_OPEN_DB_FILE;
  }

  fclose(file);
  return KVDBLITE_SUCCESS;
}

static int add_transaction(struct avltree *avl, uint8_t op, avl_key_t *key, avl_value_t *value) {
  FILE *file = fopen(avl->journalname, "ab");
  if (!file) {
    return KVDBLITE_FAILED_TO_OPEN_DB_FILE;
  }

  // INSERT or DELETE or DONE
  fwrite_uint8_t(op, file);

  // KEY
  fwrite_uint32_t((uint32_t)strlen(key), file);
  fwrite_str(key, file);

  if (op == KVDBLITE_OP_INSERT) {
    // VALUE
    fwrite_uint32_t((uint32_t)strlen(value), file);
    fwrite_str(value, file);
  }

  fclose(file);
  return 0;
}

static int debug_dump_transactions(char *journalname) {
  uint32_t l;
  uint8_t *v;

  FILE *file = fopen(journalname, "r");
  if (!file) {
    return KVDBLITE_FAILED_TO_OPEN_DB_FILE;
  }

  uint8_t op;

  while (1) {
    if (fread_uint8_t(&op, file) < 0) {
      fclose(file);
      return 1;
    }

    switch (op) {
      case KVDBLITE_OP_INSERT:
        printf("INSERT: ");
        break;
      case KVDBLITE_OP_REMOVE:
        printf("REMOVE: ");
        break;
      default:
        // All KVDBLITE_OP_ codes are characters for easy debug
        printf("UNKNOWN %c: ", op);
    }

    // Key
    if (fread_uint32_t(&l, file) < 0) {
      fclose(file);
      return -1;
    }
    printf("Len: %d ", l);
    if (fread_str(&v, l, file) < 0) {
      fclose(file);
      return -1;
    }
    printf("%s\n", v);

    if (op == KVDBLITE_OP_INSERT){
      // Value
      if (fread_uint32_t(&l, file) < 0) {
        fclose(file);
        return -1;
      }
      printf("Len: %d ", l);
      if (fread_str(&v, l, file) < 0) {
        fclose(file);
        return -1;
      }
      printf("%s\n", v);
    }
  }

  fclose(file);
}

static int apply_all_transactions(struct avltree *avl) {
  uint32_t l;
  uint8_t *v, *key, *value;
  
  FILE *file = fopen(avl->journalname, "r+b");
  if (!file) {
    return KVDBLITE_FAILED_TO_OPEN_DB_FILE;
  }

  uint8_t op;

  while (1) {
    if (fread_uint8_t(&op, file) < 0) {
      fclose(file);
      return KVDBLITE_SUCCESS;
    }

    // Key
    if (fread_uint32_t(&l, file) < 0) {
      fclose(file);
      return KVDBLITE_UNEXPECTED_EOF;
    }
    if (fread_str(&v, l, file) < 0) {
      fclose(file);
      return KVDBLITE_UNEXPECTED_EOF;
    }
    key = strdup(v);
    free(v);

    if (op == KVDBLITE_OP_INSERT) {
      // Value
      if (fread_uint32_t(&l, file) < 0) {
        fclose(file);
        return KVDBLITE_UNEXPECTED_EOF;
      }
      if (fread_str(&v, l, file) < 0) {
        fclose(file);
        return KVDBLITE_UNEXPECTED_EOF;
      }
      value = strdup(v);
      free(v);
      
      insert(key, value, &avl->root);

      free(key);
      free(value);
    } else {
      // KVDBLITE_OP_REMOVE
      remove_(key, &avl->root);

      free(key);
    }
  }
  fclose(file);

  return KVDBLITE_SUCCESS;
}

//
// End Journalling
//

//
// AVL tree internals
//

static inline int max(int a, int b) { return a > b ? a : b; }

static inline int min(int a, int b) { return a < b ? a : b; }

static inline void fix_diffs_right(int *ap, int *bp) {
  int a = *ap, b = *bp, k = (b > 0) * b;
  *ap = k + (a - b) + 1;
  *bp = max(b, k + a + 1) + 1;
}

static inline void fix_diffs_left(int *ap, int *bp) {
  int a = *ap, b = *bp, k = (b < 0) * b;
  *ap = k + (a - b) - 1;
  *bp = min(b, k + a - 1) - 1;
}

static inline void rotate_right(struct node **rp) {
  struct node *a = *rp, *b = a->left;
  a->left = b->right;
  b->right = a;
  fix_diffs_right(&a->diff, &b->diff);
  *rp = b;
}

static inline void rotate_left(struct node **rp) {
  struct node *a = *rp, *b = a->right;
  a->right = b->left;
  b->left = a;
  fix_diffs_left(&a->diff, &b->diff);
  *rp = b;
}

static inline int balance(struct node **rp) {
  struct node *a = *rp;
  if (a->diff == 2) {
    if (a->right->diff == -1)
      rotate_right(&a->right);
    rotate_left(rp);
    return 1;
  } else if (a->diff == -2) {
    if (a->left->diff == 1)
      rotate_left(&a->left);
    rotate_right(rp);
    return 1;
  }
  return 0;
}

static int insert_leaf(avl_key_t *key, avl_value_t *value, struct node **rp) {
  struct node *a = (*rp = malloc(sizeof *a));
  if (a == NULL) {
    return KVDBLITE_FAILED_TO_ALLOC_MEMORY;
  }
  a->left = a->right = NULL;
  a->diff = 0;
  a->key = strdup(key);
  a->value = strdup(value);
  return 1;
}

static int insert(avl_key_t *key, avl_value_t *value, struct node **rp) {
  struct node *a = *rp;
  if (a == NULL)
    return insert_leaf(key, value, rp);
  if (strcmp(key, a->key) == 0) {
    // Key already exists
    free(a->value);
    a->value = strdup(value);
    return 0; // Tree structure didn't change
  }
  if (strcmp(key, a->key) > 0)
    if (insert(key, value, &a->right) && (++a->diff) == 1)
      return 1;
  if (strcmp(key, a->key) < 0)
    if (insert(key, value, &a->left) && (--a->diff) == -1)
      return 1;
  if (a->diff != 0)
    balance(rp);
  return 0;
}

static int unlink_left(struct node **rp, struct node **lp) // with *rp != NULL
{
  struct node *a = *rp;
  if (a->left == NULL) {
    *rp = a->right;
    *lp = a;
    return 1;
  }
  if (unlink_left(&a->left, lp) && (++a->diff) == 0)
    return 1;
  if (a->diff != 0)
    return balance(rp) && (*rp)->diff == 0;
  return 0;
}

static int remove_root(struct node **rp) {
  int delta;
  struct node *a = *rp, *b;
  if (a->left == NULL || a->right == NULL) {
    *rp = a->right == NULL ? a->left : a->right;
    free(a->key);
    free(a->value);
    free(a);
    return 1;
  }
  delta = unlink_left(&a->right, rp);
  b = *rp;
  b->left = a->left;
  b->right = a->right;
  b->diff = a->diff;

  free(a->key);
  free(a->value);
  free(a);
  if (delta && (--b->diff) == 0)
    return 1;
  if (b->diff != 0)
    return balance(rp) && (*rp)->diff == 0;
  return 0;
}

static int remove_(avl_key_t *key, struct node **rp) {
  struct node *a = *rp;
  if (a == NULL)
    return 0;
  if (strcmp(key, a->key) == 0)
    return remove_root(rp);
  if (strcmp(key, a->key) > 0)
    if (remove_(key, &a->right) && (--a->diff) == 0)
      return 1;
  if (strcmp(key, a->key) < 0)
    if (remove_(key, &a->left) && (++a->diff) == 0)
      return 1;
  if (a->diff != 0)
    return balance(rp) && (*rp)->diff == 0;
  return 0;
}

static void free_(struct node *a) {
  if (a == NULL)
    return;
  free_(a->left);
  free_(a->right);
  free(a);
}

void zaptree_root_rm_method(struct avltree *avl) {
  while (avl->root != NULL) {
    avl_remove(avl, avl->root->key);
  }
}

struct node *leaf_left(struct node *root) {
  if (root == NULL) {
    return NULL;
  }

  leaf_left(root->left);
  return root;
}

struct node *leaf_right(struct node *root) {
  if (root == NULL) {
    return NULL;
  }

  leaf_right(root->right);
  return root;
}

static void zap_tree_traversal(struct avltree *avl) {
  struct node *lr;
  while (avl->root != NULL) {
    lr = leaf_left(avl->root);
    if (lr != NULL)
      avl_remove(avl, lr->key);
    lr = leaf_right(avl->root);
    if (lr != NULL)
      avl_remove(avl, lr->key);
  }
}

static int valid(struct node *a) {
  int lh, rh, b;
  if (a == NULL)
    return 0;
  lh = valid(a->left);
  if(lh < 0)
    return lh;
  rh = valid(a->right);
  if(rh < 0)
    return rh;
  b = rh - lh;

  if (b != a->diff) {
    printf("b %d, a->diff %d, rh %d, lh %d - a->key %s\n", b, a->diff, rh, lh,
           a->key);
    return KVDBLITE_INTERNAL_BALANCE_ERR;
  }
  if (abs(b) > 1)
    return KVDBLITE_LOPSIDED_ERR;

  return max(lh, rh) + 1;
}

// inorder traversal of the tree
static void inorder(struct node *root) {
  if (root == NULL) {
    return;
  }

  inorder(root->left);
  printf("%s: %s (%d)\n", root->key, root->value, root->diff);
  inorder(root->right);
}

// Too much recursion for large databases? Over 32K???
static int inorder_count(struct node *root) {
  if (root == NULL) {
    return 0;
  }

  int c = 0;

  c = c + inorder_count(root->left);
  c++;
  c = c + inorder_count(root->right);

  return c;
}

//
// END AVL tree internals
//

//
// AVL tree public API
//

void avl_insert(struct avltree *avl, avl_key_t *key, avl_value_t *value) {
  if(avl->journalname!=NULL)
    add_transaction(avl, KVDBLITE_OP_INSERT, key, value);
  insert(key, value, &avl->root);
}

void avl_remove(struct avltree *avl, avl_key_t *key) {
  if(avl->journalname!=NULL)
    add_transaction(avl, KVDBLITE_OP_REMOVE, key, NULL);
  remove_(key, &avl->root);
}

// search a node in the AVL tree
struct node *avl_search(avl_key_t *key, struct node *root) {
  if (root == NULL) {
    return NULL;
  }

  if (strcmp(root->key, key) == 0) {
    return root;
  }

  if (strcmp(key, root->key) > 0) {
    avl_search(key, root->right);
  } else {
    avl_search(key, root->left);
  }
}

struct avl_lookup_result *avl_lookup(struct avltree *avl, avl_key_t *key) {
  struct avl_lookup_result *r = NULL;
  struct node *n = avl_search(key, avl->root);
  if(n==NULL) {
    return NULL;
  } else {
    r = malloc(sizeof *r);
    if(r == NULL) {
      return NULL;
    }

    r->key = strdup(n->key);
    r->value = strdup(n->value);
  }
}

void avl_free_lookup_result(struct avl_lookup_result *r) {
  free(r->key);
  free(r->value);
  free(r);
}

void avl_free(struct avltree *avl) {
  free_(avl->root);
  if(avl->dbname!=NULL)
    free(avl->dbname);
  if(avl->journalname!=NULL)
    free(avl->journalname);
  free(avl);
}

struct avltree *avl_make(uint8_t *fn) {
  generate_CRC32_table();
  struct avltree *avl = malloc(sizeof *avl);
  if (avl == NULL) {
    return NULL;
  }
  avl->root = NULL;
  if(fn==NULL) {
    avl->dbname = NULL;
    avl->journalname = NULL;
  } else {
    avl->dbname = strdup(fn);
    avl->journalname = malloc(strlen(fn) + 5);
    sprintf(avl->journalname, "%s%s", fn, ".jnl");
  }

  // Load the tree from disk if the file exists
  if(fn!=NULL) {
    load_avl_tree(avl, fn);
  }

  // Apply any transactions from the journal
  apply_all_transactions(avl);

  return avl;
}

int avl_check_valid(struct avltree *avl) { return valid(avl->root); }

int avl_db_size(struct avltree *avl) {
  struct node *root = avl->root;
  if (root == NULL) {
    return 0;
  }

  int c = 0;
  c = c + inorder_count(root->left);
  c = c + inorder_count(root->right);
  return c + 1;
}

// extended inorder traversal of the tree
void avl_debug_inorder(struct avltree *avl) {
  struct node *root = avl->root;
  printf("--\n");
  if (root == NULL) {
    printf("Empty!\n");
    return;
  }

  printf("Left:\n");
  inorder(root->left);
  printf("Root:\n");
  printf("%s: %s (%d)\n", root->key, root->value, root->diff);
  printf("Right:\n");
  inorder(root->right);
}

//
// END AVL tree public API
//