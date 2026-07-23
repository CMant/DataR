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
#include <fcntl.h>

#define PAGE_SIZE 16384
#define MAX_TUPLES_PER_BATCH 100

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
static int unblock_g_reply_sock; /* 要发送的socket（非阻塞上报） */
static char g_reply_msg[256];    /* 要发送的内容 */
static int block_g_reply_sock;   /* 阻塞回复 socket（出错立即退出用） */

/* 源端 PG 连接（导出让 gen_result 回调可用） */
PGconn *pg_engine_source_conn = NULL;

/* 信号量与缓冲区（引擎内部管理） */
static sem_t *sem_null;
static sem_t *sem_full;
static int max_message_size;

/* 外部速度控制参数 */
extern int deceleration_rounds;
extern int calculate_speed_rounds;

/* 引擎运行时需要的上下文（避免参数传递过多） */
static const char *g_task_name = NULL;

static pthread_mutex_t mtx = PTHREAD_MUTEX_INITIALIZER;
static int pthread_mutex_exit_count = 0;
static pthread_cond_t cond = PTHREAD_COND_INITIALIZER;
static int replay_message_exit_sig = 0;
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

    	if (replay_message_exit_sig)
		{
			exit(0);
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
typedef struct
{
    int tidnum;
    int res_count; /* 当前批次有效 res 数量 */
    int finish_tag;
    int max_tuples;              /* res_arr 容量（MAX_TUPLES_PER_BATCH）*/
    PGresult **res_arr;          /* PGresult* 指针数组，存放主线程传入的 res */
    void *write_ctx;             /* write_init 返回的上下文 */
    const PgEngineCallbacks *cb; /* 回调集 */
} ThreadPara;

static void *write_task_wrapper(void *arg)
{
    ThreadPara *tp = (ThreadPara *)arg;
    const PgEngineCallbacks *cb = tp->cb;

    /* 调用回调初始化目标端 */
    tp->write_ctx = cb->write_init(tp->tidnum, g_task_name,
                                   block_g_reply_sock);

    /* 预分配本地格式化缓冲区 */
    int local_buf_size = max_message_size;
    char *local_buf = (char *)calloc(local_buf_size, 1);
    if (!local_buf)
    {
        snprintf(g_reply_msg, 256, "task_name:%s thread:%d local_buf alloc failed\n",
                 g_task_name, tp->tidnum);
        ReplyToClient(block_g_reply_sock, g_reply_msg);
        cb->write_fini(tp->tidnum, tp->write_ctx);

        pthread_mutex_lock(&mtx);
        pthread_mutex_exit_count++;
        pthread_cond_signal(&cond);
        pthread_mutex_unlock(&mtx);
        return NULL;
    }

    while (1)
    {
        sem_wait(&sem_full[tp->tidnum]);

        if (tp->finish_tag == 1)
        {
            snprintf(g_reply_msg, 256, "task_name:%s thread:%d write task finish\n",
                     g_task_name, tp->tidnum);
            ReplyToClient(block_g_reply_sock, g_reply_msg);
            break;
        }

        /* 将批次内的所有 PGresult 格式化为本地缓冲区 */
        char *result_ptr = local_buf;
        int message_size = 0;

        for (int i = 0; i < tp->res_count; i++)
        {
            void *escape_conn = NULL;
            if (cb->get_escape_conn)
                escape_conn = cb->get_escape_conn(tp->tidnum, tp->write_ctx);

            int len = cb->gen_result(tp->res_arr[i], escape_conn, &local_buf);
            // printf("%d\n",len);
            message_size += len;
            // printf("%s\n",result_ptr);
            if (message_size * 2 > max_message_size)
            {

                if (cb->write_exec(tp->tidnum, tp->write_ctx,
                                   result_ptr, message_size) != 0)
                {
                    snprintf(g_reply_msg, sizeof(g_reply_msg),
                             "task_name:%s thread:%d write_exec error\n",
                             g_task_name, tp->tidnum);
                    ReplyToClient(block_g_reply_sock, g_reply_msg);
                    goto WRITE_ERROR;
                }
                local_buf = result_ptr;
                message_size = 0;
            }
        }

        /* 写出剩余数据 */
        if (message_size > 0)
        {

            if (cb->write_exec(tp->tidnum, tp->write_ctx,
                               result_ptr, message_size) != 0)
            {
                snprintf(g_reply_msg, sizeof(g_reply_msg),
                         "task_name:%s thread:%d last res write_exec error\n",
                         g_task_name, tp->tidnum);
                ReplyToClient(block_g_reply_sock, g_reply_msg);
                goto WRITE_ERROR;
            }
            local_buf = result_ptr;
            message_size = 0;
        }

        /* 清理本批次的 PGresult */
        for (int i = 0; i < tp->res_count; i++)
        {
            PQclear(tp->res_arr[i]);
            tp->res_arr[i] = NULL;
        }
        tp->res_count = 0;

        sem_post(&sem_null[tp->tidnum]);
    }

    free(local_buf);
    sem_post(&sem_null[tp->tidnum]);
    cb->write_fini(tp->tidnum, tp->write_ctx);

    pthread_mutex_lock(&mtx);
    pthread_mutex_exit_count++;
    pthread_cond_signal(&cond);
    pthread_mutex_unlock(&mtx);
    return NULL;

WRITE_ERROR:
    for (int i = 0; i < tp->res_count; i++)
    {
        PQclear(tp->res_arr[i]);
        tp->res_arr[i] = NULL;
    }
    tp->res_count = 0;
    free(local_buf);
    sem_post(&sem_null[tp->tidnum]);
    cb->write_fini(tp->tidnum, tp->write_ctx);

    pthread_mutex_lock(&mtx);
    pthread_mutex_exit_count++;
    pthread_cond_signal(&cond);
    pthread_mutex_unlock(&mtx);
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
        !cb->write_exec || !cb->write_fini)
    {
        fprintf(stderr, "pg_engine: 回调函数集或参数不完整\n");
        snprintf(g_reply_msg, 256, "pg_engine: 回调函数集或参数不完整\n");
        ReplyToClient(block_g_reply_sock, g_reply_msg);
        return -1;
    }

    block_g_reply_sock = acceptSockfd;
    g_task_name = task_name;

    if (parallel_thread_per_task <= 0)
        parallel_thread_per_task = 1;
    if (send_buffer_size_val <= 0)
        send_buffer_size_val = 1024;

    /* 重置线程退出计数器 */
    pthread_mutex_exit_count = 0;

    /* ---- 1. 启动回复线程 ---- */
    pthread_create(&g_reply_tid, NULL, reply_client_thread, NULL);

    /* ---- 2. 分配信号量和缓冲区 ---- */
    int page_sz = (cb->page_size > 0) ? cb->page_size : PAGE_SIZE;
    max_message_size = send_buffer_size_val * page_sz * 2;

    sem_null = (sem_t *)calloc(parallel_thread_per_task, sizeof(sem_t));
    sem_full = (sem_t *)calloc(parallel_thread_per_task, sizeof(sem_t));

    ThreadPara *thread_para = (ThreadPara *)calloc(parallel_thread_per_task,
                                                   sizeof(ThreadPara));
    pthread_t *tidW = (pthread_t *)calloc(parallel_thread_per_task,
                                          sizeof(pthread_t));

    for (int s = 0; s < parallel_thread_per_task; s++)
    {
        thread_para[s].res_arr = (PGresult **)calloc(MAX_TUPLES_PER_BATCH,
                                                     sizeof(PGresult *));
        if (thread_para[s].res_arr == NULL)
        {
           // fprintf(stderr, "第 %d 个线程的 res_arr 申请失败！\n", s);
            snprintf(g_reply_msg, 256, "第 %d 个线程的 res_arr 申请失败！\n", s);
            ReplyToClient(block_g_reply_sock, g_reply_msg);
            exit(-1);
        }

        sem_init(&sem_null[s], 0, 1);
        sem_init(&sem_full[s], 0, 0);

        thread_para[s].tidnum = s;
        thread_para[s].res_count = 0;
        thread_para[s].finish_tag = 0;
        thread_para[s].max_tuples = MAX_TUPLES_PER_BATCH;
        thread_para[s].write_ctx = NULL;
        thread_para[s].cb = cb;

        pthread_create(&tidW[s], NULL, write_task_wrapper, &thread_para[s]);
    }

    /* ---- 3. 连接 PG 源端，singleRow 模式 ---- */
    PGconn *source_conn = get_pg_source_conn(source_conninfo);
    pg_engine_source_conn = source_conn;
    PGresult *res;

    int sendstatus = PQsendQuery(source_conn, pump_sql);
    int singlemode = PQsetSingleRowMode(source_conn);

    if (sendstatus != 1 && singlemode != 1)
    {
        snprintf(g_reply_msg, sizeof(g_reply_msg), "single_MODE 配置失败\n");
        ReplyToClient(block_g_reply_sock, g_reply_msg);
    }

    /* ---- 4. 主循环：读取PG → res_arr 分批 → 切换线程（含速度控制） ---- */
    int g = 0;
    int res_count_in_batch = 0;
    int dec = 0;
    int turn_i = 0;
    uint64_t row_count = 0;
    int need_continue = 1;
    int res_value_count=0;
    struct timeval start_time, stop_time;
    float duration_time;

    while (1)
    {
        sem_wait(&sem_null[g]);

        while (need_continue)
        {
            if (turn_i == 0)
                gettimeofday(&start_time, NULL);

            res = PQgetResult(source_conn);

            if (NULL == res)
            {
                printf("%s\n", "NO_RESULT");
                need_continue = 0;
                break;
            }
            res_value_count=  PQntuples(res);  
            row_count += res_value_count;
            thread_para[g].res_arr[res_count_in_batch] = res;
            res_count_in_batch += res_value_count;
            thread_para[g].res_count = res_count_in_batch;
             if (res_count_in_batch == MAX_TUPLES_PER_BATCH)
            {
                res_count_in_batch = 0;
                sem_post(&sem_full[g]);
                break;
            }

     //计算速度
            ++turn_i;
            ++dec;
            if (dec == deceleration_rounds)
            {
                usleep(fast_table_get(task_name) * 1000);
                dec = 0;
            }
            if (turn_i == calculate_speed_rounds)
            {
                gettimeofday(&stop_time, NULL);
                duration_time = (stop_time.tv_sec - start_time.tv_sec) + (stop_time.tv_usec - start_time.tv_usec) / 1000000.0f;
                snprintf(g_reply_msg, sizeof(g_reply_msg),
                         "task_name:%s %fr/s  task_speed:%ld row_count:%" PRIu64 "\n",
                         task_name,
                         calculate_speed_rounds / duration_time,
                         fast_table_get(task_name),
                         row_count);
                unblock_g_reply_sock = acceptSockfd;
                turn_i = 0;
            }
            //计算速度
           
        }
         g++;
            if (g == parallel_thread_per_task)
                g = 0;
        /* 如果 PG 已无数据，不再继续分配 */
        if (!need_continue)
        {
           
           
            break;
        }

       
    }

    /* ---- 5. 通知所有写入线程结束 ---- */
    for (int s = 0; s < parallel_thread_per_task; s++)
    {
        thread_para[s].finish_tag = 1;
        sem_post(&sem_full[s]);
    }

    /* ---- 6. 等待 PG 源端读取完成 ---- */
    PQfinish(source_conn);

    /* ---- 7. 等待所有写入线程退出 ---- */
    // pthread_mutex_lock(&mtx);
    while (pthread_mutex_exit_count < parallel_thread_per_task)
    {
        usleep(1000);
    }
    // pthread_mutex_unlock(&mtx);
    PQclear(res);
    snprintf(g_reply_msg, sizeof(g_reply_msg),
             "task_name:%s total: %ld rows\n", task_name, row_count);

     ReplyToClient(block_g_reply_sock, g_reply_msg);

    /* ---- 8. 释放资源 ---- */
    for (int s = 0; s < parallel_thread_per_task; s++)
    {

        sem_destroy(&sem_null[s]);
        sem_destroy(&sem_full[s]);
    }

    free(sem_null);
    free(sem_full);
    free(thread_para);
    free(tidW);
    replay_message_exit_sig = 1;
    g_task_name = NULL;
    return 0;
}