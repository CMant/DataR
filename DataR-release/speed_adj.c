#include <stdio.h>
#include <sys/shm.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <unistd.h>
#include <sys/types.h>
#include <fcntl.h>
#include <string.h>
#include <errno.h>
#include "include/message_type.h"
#include "include/speed_adj.h"

extern int send_to_center_sockfd;

#define MAX_CLEAN_TASK 1024
static char g_clean_list[MAX_CLEAN_TASK][256];
static int  g_clean_cnt = 0;

// 内部函数：记录需要清理的共享内存
static void add_shm_to_clean(const char *name)
{
    if (!name) return;
    if (g_clean_cnt >= MAX_CLEAN_TASK) return;

    // 已存在则不重复添加
    for (int i = 0; i < g_clean_cnt; i++) {
        if (strcmp(g_clean_list[i], name) == 0) {
            return;
        }
    }

    strncpy(g_clean_list[g_clean_cnt], name, 255);
    g_clean_list[g_clean_cnt][255] = 0;
    g_clean_cnt++;
}

// 全局清理所有共享内存（退出时调用）
void clean_all_shm(void)
{
    for (int i = 0; i < g_clean_cnt; i++) {
        shm_unlink(g_clean_list[i]);
    }
    g_clean_cnt = 0;
}

// ==========================
// 设置sleep值（可无限次反复调用）
// ==========================
int Set_sleepValue(int sleep_value, char *taskname) {
    int fd = shm_open(taskname, O_CREAT | O_RDWR, 0666);
    if (fd < 0) {
        perror("shm_open failed");
        return -1;
    }

    ftruncate(fd, 16);
    char *p = mmap(NULL, 16, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (p == MAP_FAILED) {
        perror("mmap failed");
        close(fd);
        return -1;
    }

    snprintf(p, 16, "%d", sleep_value);
    munmap(p, 16);
    close(fd);

    // 记录：退出时自动清理
    add_shm_to_clean(taskname);
    return 0;
}

// ==========================
// 读取sleep值
// ==========================
int Get_sleepValue(char *taskname) {
    int fd = shm_open(taskname, O_RDONLY, 0666);
    if (fd < 0) {
        return -1;
    }

    char *p = mmap(NULL, 16, PROT_READ, MAP_SHARED, fd, 0);
    if (p == MAP_FAILED) {
        close(fd);
        return -1;
    }

    int sleep_time = atoi(p);
    munmap(p, 16);
    close(fd);

    return sleep_time;
}

// ==========================
// 删除共享内存
// ==========================
int Del_sleep_tag(char *taskname) {
    return shm_unlink(taskname);
}