#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/wait.h>
#include "include/adj_speed.h"

int main()
{
    printf("==== 初始化共享内存 ====\n");
    fast_table_init();

    fast_table_set("pg_to_mysql", 100000);
    printf("父进程设置: pg_to_mysql = 100000\n\n");

    pid_t pid = fork();
    if (pid == 0) {
        printf("【子进程 %d 】\n", getpid());
        printf("find_index:%d\n",find_index("pg_to_mysql1"));
        uint64_t speed = fast_table_get("pg_to_mysql");
        printf("子进程读取 = %lu\n", speed);
        
        fast_table_set("pg_to_mysql", 50000);
        printf("子进程修改为 50000\n");
        exit(0);
    }

    wait(NULL);
    printf("\n【父进程读取最终值】\n");
    uint64_t final_speed = fast_table_get("pg_to_mysql");
    printf("最终速度 = %lu\n", final_speed);

    fast_table_traverse();
    fast_table_cleanup_all();
    return 0;
}