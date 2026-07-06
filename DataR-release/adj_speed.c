#include "include/adj_speed.h"
#include <sys/shm.h>
#include <sys/ipc.h>
#include <stdio.h>
#include <string.h>

#define SHM_KEY 0x56789ABC
#define SHM_SIZE (sizeof(TaskItem) * MAX_TASK)

// 挂载共享内存
static TaskItem *shm_attach(int create)
{
    int shmid;
    if (create) {
        shmid = shmget(SHM_KEY, SHM_SIZE, IPC_CREAT | 0666);
    } else {
        shmid = shmget(SHM_KEY, SHM_SIZE, 0666);
    }

    if (shmid < 0) return NULL;
    TaskItem *table = (TaskItem *)shmat(shmid, NULL, 0);
    if (table == (void *)-1) return NULL;

    return table;
}

// 卸载
static void shm_detach(TaskItem *table)
{
    if (table) shmdt(table);
}

// ==============================
// 初始化（主进程）
// ==============================
void fast_table_init(void)
{
    TaskItem *table = shm_attach(1);
    if (!table) return;
    memset(table, 0, SHM_SIZE);
    shm_detach(table);
}

// ==============================
// 查找索引
// ==============================
int find_index(const char *name)
{
    if (!name) return -1;

    TaskItem *table = shm_attach(0);
    if (!table) return -1;

    int idx = -1;
    for (int i = 0; i < MAX_TASK; i++) {
        if (table[i].used && strcmp(table[i].name, name) == 0) {
            idx = i;
            break;
        }
    }

    shm_detach(table);
    return idx;
}

// ==============================
// 设置
// ==============================
int fast_table_set(const char *task_name, uint64_t speed)
{
    if (!task_name) return -1;

    TaskItem *table = shm_attach(0);
    if (!table) return -1;

    int idx = find_index(task_name);
    if (idx >= 0) {
        table[idx].speed = speed;
        shm_detach(table);
        return 0;
    }

    for (int i = 0; i < MAX_TASK; i++) {
        if (!table[i].used) {
            strncpy(table[i].name, task_name, 63);
            table[i].name[63] = 0;
            table[i].speed = speed;
            table[i].used = 1;
            shm_detach(table);
            return 0;
        }
    }

    shm_detach(table);
    return -1;
}


int fast_table_set_pid(const char *task_name, uint64_t speed,const pid_t pid)
{
    if (!task_name) return -1;

    TaskItem *table = shm_attach(0);
    if (!table) return -1;

    int idx = find_index(task_name);
    if (idx >= 0) {
        table[idx].speed = speed;
        shm_detach(table);
        return 0;
    }

    for (int i = 0; i < MAX_TASK; i++) {
        if (!table[i].used) {
            strncpy(table[i].name, task_name, 63);
            table[i].name[63] = 0;
            table[i].speed = speed;
            table[i].used = 1;
            table[i].pid=pid;
            shm_detach(table);
            return 0;
        }
    }

    shm_detach(table);
    return -1;
}



// ==============================
// 获取
// ==============================
uint64_t fast_table_get(const char *task_name)
{
    if (!task_name) return UINT64_MAX;

    TaskItem *table = shm_attach(0);
    if (!table) return UINT64_MAX;

    int idx = find_index(task_name);
    if (idx < 0) {
        shm_detach(table);
        return UINT64_MAX;
    }

    uint64_t val = table[idx].speed;
    shm_detach(table);
    return val;
}

// ==============================
// 删除
// ==============================
int fast_table_del(const char *task_name, const pid_t pid)
{
    if (!task_name) return -1;

    TaskItem *table = shm_attach(0);
    if (!table) return -1;

    // ==============================
    // 原来只按名字找
    // 现在：找 **name + pid 都一致** 的条目
    // ==============================
    int idx = -1;
    for (int i = 0; i < MAX_TASK; i++) {
        if (table[i].used == 1) {
            if (strcmp(table[i].name, task_name) == 0 && table[i].pid == pid) {
                idx = i;
                break;
            }
        }
    }

    if (idx < 0) {
        shm_detach(table);
        return -1;
    }

    // 只有 名称 + PID 都一致，才会走到这里删除
    table[idx].used = 0;
    memset(table[idx].name, 0, sizeof(table[idx].name));
    table[idx].speed = 0;
    table[idx].pid   = 0;  // 清空PID

    shm_detach(table);
    return 0;
}

// ==============================
// 遍历
// ==============================
void fast_table_traverse(void)
{
    TaskItem *table = shm_attach(0);
    if (!table) return;

    for (int i = 0; i < MAX_TASK; i++) {
        if (table[i].used) {
            printf("[FAST] task: %-20s speed: %lu\n", table[i].name, table[i].speed);
        }
    }

    shm_detach(table);
}

// ==============================
// 销毁共享内存
// ==============================
void fast_table_cleanup_all(void)
{
    int shmid = shmget(SHM_KEY, SHM_SIZE, 0666);
    if (shmid >= 0) {
        shmctl(shmid, IPC_RMID, NULL);
    }
}