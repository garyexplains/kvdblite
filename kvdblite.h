#ifndef KVDBLITE_H
#define KVDBLITE_H

#define KVDBLITE_SUCCESS 0
#define KVDBLITE_INTERNAL_BALANCE_ERR -1001
#define KVDBLITE_LOPSIDED_ERR -1002
#define KVDBLITE_DBNAME_IS_NULL -1003
#define KVDBLITE_FAILED_TO_OPEN_DB_FILE -1004
#define KVDBLITE_FAILED_TO_ALLOC_MEMORY -1005


typedef uint8_t avl_key_t;
typedef uint8_t avl_value_t;

struct avltree;

struct avltree *avl_make(uint8_t *);
void avl_free(struct avltree *);

void avl_insert(struct avltree *, avl_key_t *, avl_value_t *);
void avl_remove(struct avltree *, avl_key_t *);
struct node *avl_lookup(struct avltree *, avl_key_t *);
int avl_check_valid(struct avltree *);
void avl_debug_inorder(struct avltree *);
int avl_save_database(struct avltree *);
int avl_db_size(struct avltree *avl);

#endif /* KVDBLITE_H */