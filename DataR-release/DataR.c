#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <sys/wait.h>
#include <locale.h>
#include <signal.h>

 
#include "include/network_service.h"
#include "include/ctl_command.h"
#include "include/init_check.h"
#include "include/adj_speed.h"
// 全局标记
static int g_exit_flag = 0;
static pid_t g_child_pid = -1;

/* 回收子进程 */
void sig_chld(int sig)
{
	(void)sig;
    while (waitpid(-1, NULL, WNOHANG) > 0);
}

// Ctrl+C 安全退出：清理共享内存 + 杀死所有进程
void sig_int(int sig)
{
    (void)sig;
    printf("\n✅ 收到 Ctrl+C，正在安全退出...\n");

    g_exit_flag = 1;

    // 清理所有共享内存（关键修复）
    fast_table_cleanup_all();

    // 杀死整个进程组
    kill(0, SIGTERM);
    usleep(300000);
    kill(0, SIGKILL);

    printf("✅ 程序已安全退出\n");
    exit(0);
}

/* 信号设置 */
void set_signals(void)
{
    signal(SIGPIPE, SIG_IGN);
    signal(SIGCHLD, sig_chld);
    signal(SIGINT,  sig_int);
    signal(SIGTERM, sig_int);
}

int main(int argc, char *argv[])
{
	(void)argc;
	(void)argv;
    setlocale(LC_ALL, "");

    init_check();
    set_signals();
    fast_table_init();
    while (!g_exit_flag)
    {
        pid_t pid = fork();

        if (pid == -1)
        {
            perror("fork failed");
            sleep(1);
            continue;
        }

        if (pid == 0)
        {
            g_child_pid = getpid();
            ServerProcess();
            exit(-1);
        }
        else
        {
            g_child_pid = pid;
            waitpid(pid, NULL, 0);

            if (g_exit_flag) {
                break;
            }

            printf("子进程崩溃，自动重启...\n");
            sleep(1);
        }
    }

    return 0;
}