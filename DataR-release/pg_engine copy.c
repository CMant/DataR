#include "include/pg_engine.h"
#include "include/common.h"
#include "include/adj_speed.h"
#include "include/network_service.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <unistd.h>
#include <time.h>
#include <sys/time.h>
#include <inttypes.h>

/* ===================================================================
 *  pg_engine.c — 通用 PG→任意目标 迁移引擎实现
 *
 *  功能：将 pg_to_xxx.c 中完全重复的如下逻辑提取到此处：
 *    - 回复线程 (reply_client_thread)
 *    - 信号量、并发缓冲区分配与销毁
 *    - PG 源端连接、singleRow 模式查询
 *    - 主读取循环（含速度控制、缓冲区满切换、统计上报）
 *    - 资源清理
 *
 *  引擎不关心 TASK_INF——各 pg_to_xx.c 自行定义管理配置。
 * =================================================================== */

/* ---------- 全局变量 ---------- */

/* 异步回复客户端专用（只开1个线程） */
static pthread_t g_reply_tid;
static int       unblock_g_reply_sock;    /* 要发送的socket（非阻塞上报） */
static char      g_reply_msg[256];        /* 要发送的内容 */
static int       block_g_reply_sock;      /* 阻塞回复 socket（出错立即退出用） */

/* 源端 PG 连接（导出让 gen_result 回调可用） */
PGconn *pg_engine_source_conn = NULL;

/* 信号量与缓冲区（引擎内部管理） */
static sem_t  *sem_null;
static sem_t  *sem_full;
static char  **send_buffer;
static int     max_message_size;
static int     keep_size_buffer;

/* 外部速度控制参数 */
extern int deceleration_rounds;
extern int calculate_speed_rounds;

/* 引擎运行时需要的上下文（避免参数传递过多） */
static const char *g_task_name = NULL;

/* ===================================================================
 *  异步回复线程
 * =================================================================== */
static void *reply_client_thread(void *arg)
{
    (void)arg;
    while (1)
    {
        if (unblock_g_reply_sock <= 0)
        {
            usleep(1000); /* 1ms */
            continue;
        }
        ReplyToClient(unblock_g_reply_sock, g_reply_msg);
        unblock_g_reply_sock = 0;
    }
    return NULL;
}

/* ===================================================================
 *  PG 源端连接
 * =================================================================== */
static PGconn *get_pg_source_conn(const char *conninfo)
{
    PGconn *conn = PQconnectdb(conninfo);
    printf("连接数据库成功 !\n");
    char report[256] = {0};
    if (PQstatus(conn) == CONNECTION_BAD)
    {
        snprintf(report, sizeof(report),
                 "连接到数据库失败，连接地址 %s\n", PQerrorMessage(conn));
        ReplyToClient(block_g_reply_sock, report);
        exit(-1);
    }
    return conn;
}

/* ===================================================================
 *  写入线程包装器（调用回调）
 * =================================================================== */
typedef struct {
    int                tidnum;
    long int           current_message_size;
    int                finish_tag;
    void              *write_ctx;          /* write_init 返回的上下文 */
    const PgEngineCallbacks *cb;           /* 回调集 */
} ThreadPara;

static void *write_task_wrapper(void *arg)
{
    ThreadPara *tp = (ThreadPara *)arg;
    char replay_message[256];
    const PgEngineCallbacks *cb = tp->cb;

    /* 调用回调初始化目标端 */
    tp->write_ctx = cb->write_init(tp->tidnum, g_task_name,
                                   block_g_reply_sock);
    snprintf(replay_message, 256, "task_name:%s thread:%d connect_success\n",
             g_task_name, tp->tidnum);
    ReplyToClient(block_g_reply_sock, replay_message);

    while (1)
    {
        sem_wait(&sem_full[tp->tidnum]);

        if (tp->finish_tag == 1)
            break;

        /* 调用回调写入目标 */
        if (cb->write_exec(tp->tidnum, tp->write_ctx,
                           send_buffer[tp->tidnum], tp->current_message_size) != 0)
        {
            snprintf(replay_message, sizeof(replay_message),
                     "task_name:%s thread:%d write_error\n",
                     g_task_name, tp->tidnum);
            ReplyToClient(block_g_reply_sock, replay_message);
            sem_post(&sem_null[tp->tidnum]);
            cb->write_fini(tp->tidnum, tp->write_ctx);
            return NULL;
        }

        sem_post(&sem_null[tp->tidnum]);
    }

    sem_post(&sem_null[tp->tidnum]);
    cb->write_fini(tp->tidnum, tp->write_ctx);
    return NULL;
}

/* ===================================================================
 *  引擎入口
 * =================================================================== */
int pg_engine_run(int acceptSockfd,
                  const char *task_name,
                  const char *pump_sql,
                  int parallel_thread_per_task,
                  int send_buffer_size_val,
                  const char *source_conninfo,
                  const PgEngineCallbacks *cb)
{
    /* ---- 0. 参数检查 ---- */
    if (!cb || !task_name || !pump_sql || !source_conninfo ||
        !cb->gen_result || !cb->write_init ||
        !cb->write_exec || !cb->write_fini || !cb->get_buffer_prefix)
    {
        fprintf(stderr, "pg_engine: 回调函数集或参数不完整\n");
        return -1;
    }

    block_g_reply_sock = acceptSockfd;
    g_task_name = task_name;

    if (parallel_thread_per_task <= 0) parallel_thread_per_task = 1;
    if (send_buffer_size_val <= 0)     send_buffer_size_val = 1024;

    char replay_message[256];

    /* ---- 1. 启动回复线程 ---- */
    pthread_create(&g_reply_tid, NULL, reply_client_thread, NULL);

    /* ---- 2. 分配信号量和缓冲区 ---- */
    int page_sz = (cb->page_size > 0) ? cb->page_size : 8192;
    keep_size_buffer = 2 * page_sz;

    sem_null  = (sem_t *)calloc(parallel_thread_per_task, sizeof(sem_t));
    sem_full  = (sem_t *)calloc(parallel_thread_per_task, sizeof(sem_t));

    ThreadPara *thread_para = (ThreadPara *)calloc(parallel_thread_per_task,
                                                    sizeof(ThreadPara));
    pthread_t  *tidW = (pthread_t *)calloc(parallel_thread_per_task,
                                            sizeof(pthread_t));

    if (cb->use_half_buffer_check)
        max_message_size = send_buffer_size_val * page_sz * 2;
    else
        max_message_size = send_buffer_size_val * page_sz + keep_size_buffer;

    send_buffer = (char **)calloc(parallel_thread_per_task, sizeof(char *));

    const char *prefix = cb->get_buffer_prefix();
    int prefix_len = prefix ? strlen(prefix) : 0;

    for (int s = 0; s < parallel_thread_per_task; s++)
    {
        send_buffer[s] = (char *)calloc(max_message_size, 1);
        if (!send_buffer[s])
        {
            fprintf(stderr, "第 %d 个发送内存槽申请失败！请检查相关配置。\n", s);
            snprintf(replay_message, sizeof(replay_message),
                     "第 %d 个发送内存槽申请失败！请检查相关配置。\n", s);
            ReplyToClient(acceptSockfd, replay_message);
            exit(-1);
        }
        if (prefix)
            memcpy(send_buffer[s], prefix, prefix_len);

        sem_init(&sem_null[s], 0, 1);
        sem_init(&sem_full[s], 0, 0);

        thread_para[s].tidnum      = s;
        thread_para[s].current_message_size = 0;
        thread_para[s].finish_tag  = 0;
        thread_para[s].write_ctx   = NULL;
        thread_para[s].cb          = cb;

        pthread_create(&tidW[s], NULL, write_task_wrapper, &thread_para[s]);
    }

    /* ---- 3. 连接 PG 源端，singleRow 模式 ---- */
    PGconn  *source_conn = get_pg_source_conn(source_conninfo);
    pg_engine_source_conn = source_conn;
    PGresult *res;

    int sendstatus     = PQsendQuery(source_conn, pump_sql);
    int singlemode     = PQsetSingleRowMode(source_conn);

    if (sendstatus != 1 && singlemode != 1)
    {
        ReplyToClient(acceptSockfd, "single_MODE 配置失败\n");
    }

    /* ---- 4. 主循环：读取PG → gen_result → 缓冲区满/切换 ---- */
    char *Result_Point;
    int g = 0;
    int sendbuffer_init_size = prefix_len;
    int current_message_size = sendbuffer_init_size;
    int turn_i = 0;
    int tuple_len = 0;
    int dec = 0;
    uint64_t row_count = 0;

    struct timeval start_time, stop_time;
    float duration_time;

    while (1)
    {
        sem_wait(&sem_null[g]);
        Result_Point = send_buffer[g] + sendbuffer_init_size;

        while (1)
        {
            if (turn_i == 0)
                gettimeofday(&start_time, NULL);

            res = PQgetResult(source_conn);
            if (NULL == res)
            {
                if (current_message_size == sendbuffer_init_size)
                {
                    thread_para[g].finish_tag = 1;
                    sem_post(&sem_full[g]);
                    goto NO_RESULT;
                }
                else
                {
                    Result_Point = Result_Point - 1;
                    *Result_Point = '\0';
                    thread_para[g].current_message_size = current_message_size - 1;
                    sem_post(&sem_full[g]);
                    goto NO_RESULT;
                }
            }

            void *escape_conn = NULL;
            if (cb->get_escape_conn)
                escape_conn = cb->get_escape_conn(g, thread_para[g].write_ctx);

            tuple_len = cb->gen_result(res, escape_conn, &Result_Point);

            if (tuple_len != 0)
            {
                ++turn_i;
                ++dec;
                ++row_count;

                if (dec == deceleration_rounds)
                {
                    long speed_val = fast_table_get(task_name);
                    if (cb->speed_multiplier)
                        usleep(speed_val * 1000);
                    else
                        usleep(speed_val);
                    dec = 0;
                }

                if (turn_i == calculate_speed_rounds)
                {
                    gettimeofday(&stop_time, NULL);
                    duration_time = stop_time.tv_sec - start_time.tv_sec;
                    snprintf(g_reply_msg, sizeof(g_reply_msg),
                             "task_name:%s %fr/s  task_speed:%ld row_count:%" PRIu64 "\n",
                             task_name,
                             calculate_speed_rounds / duration_time,
                             fast_table_get(task_name),
                             row_count);
                    unblock_g_reply_sock = acceptSockfd;
                    turn_i = 0;
                }
            }

            current_message_size = current_message_size + tuple_len;

            int buffer_full = 0;
            if (cb->use_half_buffer_check)
                buffer_full = (current_message_size * 2 >= max_message_size);
            else
                buffer_full = ((max_message_size - current_message_size) < keep_size_buffer);

            if (buffer_full)
            {
                Result_Point = Result_Point - 1;
                *Result_Point = '\0';
                thread_para[g].current_message_size = current_message_size - 1;
                current_message_size = sendbuffer_init_size;
                break;
            }

            PQclear(res);
        }

        sem_post(&sem_full[g]);
        g++;
        if (g == parallel_thread_per_task)
            g = 0;
    }

NO_RESULT:
    /* ---- 5. 清理 ---- */
    snprintf(replay_message, sizeof(replay_message),
             "task_name:%s total: %ld rows\n", task_name, row_count);
    ReplyToClient(acceptSockfd, replay_message);

    PQfinish(source_conn);

    for (int s = 0; s < parallel_thread_per_task; s++)
    {
        sem_wait(&sem_null[s]);
        free(send_buffer[s]);
        sem_destroy(&sem_null[s]);
        sem_destroy(&sem_full[s]);
    }

    free(sem_null);
    free(sem_full);
    free(send_buffer);
    free(thread_para);
    free(tidW);

    g_task_name = NULL;
    return 0;
}