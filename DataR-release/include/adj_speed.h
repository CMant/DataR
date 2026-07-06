#ifndef TASK_FAST_TABLE_H
#define TASK_FAST_TABLE_H

#include <stdint.h>
#include <string.h>
#include <sys/types.h>
#ifdef __cplusplus
extern "C" {
#endif

#define MAX_TASK 1024

typedef struct {
    char     name[64];
    uint64_t speed;
    uint8_t  used;
    pid_t  pid;
} TaskItem;

extern TaskItem g_fast_table[MAX_TASK];

void fast_table_init(void);
int fast_table_set(const char *task_name, uint64_t speed);
int fast_table_set_pid(const char *task_name, uint64_t speed,const pid_t pid);
uint64_t fast_table_get(const char *task_name);
pid_t fast_table_del(const char *task_name,const pid_t pid);
void fast_table_traverse(void);
int find_index(const char *name);
void fast_table_cleanup_all(void);

#ifdef __cplusplus
}
#endif

#endif