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
// CRC32 in saved db file
// Only works with strings. Keys should be strings, but value should be binary with a len
// Error handling needs to be robust and consistent
// avl_import(char * fn) and avl_append(char *fn). The import needs to check that root is NULL.
// Memory protection (for struct avltree), and mutex locking for all operations

// Note: Since some functions use recursion, is there a danger of stack overflow
// and/or too deep? Tested till 100,000 and worked. Maybe no, as the depth is only 20 for 100,000 entries???

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
// DISK
//

static int fwrite_str(uint8_t *s, FILE *file) {
  size_t itemsWritten = fwrite(s, strlen(s), 1, file);
  //   if (itemsWritten != 1) {
  //     perror("Failed to write to file");
  //     fclose(file);
  //     return 1;
  // }
  return 1;
}
static int fwrite_uint32_t(uint32_t value, FILE *file) {
  // need to use network order for cross platform compatibility
  size_t itemsWritten = fwrite(&value, sizeof(uint32_t), 1, file);
  //   if (itemsWritten != 1) {
  //     perror("Failed to write to file");
  //     fclose(file);
  //     return 1;
  // }
  return 1;
}

static int fwrite_uint8_t(uint8_t value, FILE *file) {
  size_t itemsWritten = fwrite(&value, sizeof(uint8_t), 1, file);
  //   if (itemsWritten != 1) {
  //     perror("Failed to write to file");
  //     fclose(file);
  //     return 1;
  // }
  return 1;
}

static int fread_str(uint8_t **v, uint32_t l, FILE *file) {
  *v = malloc(l + 1);
  size_t itemsRead = fread(*v, l, 1, file);
  if (itemsRead != 1) {
    //        perror("Failed to read from file");
    fclose(file);
    return -1;
  }
  (*v)[l] = 0;

  return 1;
}

static int fread_uint32_t(uint32_t *value, FILE *file) {
  size_t itemsRead = fread(value, sizeof(uint32_t), 1, file);

  if (itemsRead != 1) {
    //        perror("Failed to read from file");
    fclose(file);
    return -1;
  }

  return 1;
}

static int fread_uint8_t(uint8_t *value, FILE *file) {
  size_t itemsRead = fread(value, sizeof(uint8_t), 1, file);

  if (itemsRead != 1) {
    //        perror("Failed to read from file");
    fclose(file);
    return -1;
  }

  return 1;
}

static void save_tree_to_disk(struct node *root, FILE *file) {
  if (root == NULL) {
    fwrite_uint32_t(0, file);
    return;
  }

  // Save current node's key and value
  uint32_t l = strlen(root->key);
  fwrite_uint32_t(l, file);
  fwrite_str(root->key, file);

  l = strlen(root->value);
  fwrite_uint32_t(l, file);
  fwrite_str(root->value, file);

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
  // What happens if it crashes here, after fclose but before
  // transaction file is truncated?
  truncate_transaction_file(avl);
  return KVDBLITE_SUCCESS;
}

static struct node *load_tree_from_disk(FILE *file) {
  char key[256], value[256];
  uint32_t l;
  uint8_t *v;

  if (fread_uint32_t(&l, file) < 0)
    return NULL;
  if (l == 0)
    return NULL;

  fread_str(&v, l, file);

  struct node *new_node = malloc(sizeof *new_node);
  if (!new_node) {
    perror("Failed to allocate memory for node");
    exit(EXIT_FAILURE);
  }
  new_node->key = strdup(v);
  free(v);

  if (fread_uint32_t(&l, file) < 0)
    return NULL;
  fread_str(&v, l, file);
  new_node->value = strdup(v);
  free(v);

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
    if (fread_uint32_t(&l, file) < 0)
      return -1;
    printf("Len: %d ", l);
    if (fread_str(&v, l, file) < 0)
      return -1;
    printf("%s\n", v);

    if (op == KVDBLITE_OP_INSERT){
      // Value
      if (fread_uint32_t(&l, file) < 0)
        return -1;
      printf("Len: %d ", l);
      if (fread_str(&v, l, file) < 0)
        return -1;
      printf("%s\n", v);
    }
  }
  // Need to be consistent, who closes the file?
  // And in the error case?
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
      return KVDBLITE_SUCCESS;
    }

    // Key
    if (fread_uint32_t(&l, file) < 0)
      return KVDBLITE_UNEXPECTED_EOF;
    if (fread_str(&v, l, file) < 0)
      return KVDBLITE_UNEXPECTED_EOF;
    key = strdup(v);
    free(v);

    if (op == KVDBLITE_OP_INSERT) {
      // Value
      if (fread_uint32_t(&l, file) < 0)
        return KVDBLITE_UNEXPECTED_EOF;
      if (fread_str(&v, l, file) < 0)
        return KVDBLITE_UNEXPECTED_EOF;
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
  // Need to be consistent, who closes the file?
  // And in the error case?
  fclose(file);

  return KVDBLITE_SUCCESS;
}

//
// End Journalling
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

  if (b != a->diff)
    return KVDBLITE_INTERNAL_BALANCE_ERR;
  if (abs(b) > 1)
    return KVDBLITE_LOPSIDED_ERR;

  return max(lh, rh) + 1;
}

int avl_check_valid(struct avltree *avl) { return valid(avl->root); }

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