/* ===================================================================
 *  pg_to_json.c — PG → JSON 文件迁移（基于 pg_engine）
 *
 *  TASK_INF 定义、创建、释放完全在此文件内。
 * =================================================================== */

#include "include/pg_engine.h"
#include "include/common.h"
#include "include/network_service.h"
#include "include/cJSON.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <fcntl.h>
#include <unistd.h>
#include <time.h>

/* ===================================================================
 *  本地任务配置
 * =================================================================== */
typedef struct {
    char *task_name;
    char *task_speed;
    char *migrate_type;
    char *pump_sql;
    int   parallel_thread_per_task;
    int   send_buffer_size;
    char *source_host;
    char *source_port;
    char *source_dbname;
    char *source_password;
    char *source_username;
} MyTaskCfg;

static void my_task_free(MyTaskCfg *cfg)
{
    if (!cfg) return;
    g_free(cfg->task_name);
    g_free(cfg->task_speed);
    g_free(cfg->migrate_type);
    g_free(cfg->pump_sql);
    g_free(cfg->source_host);
    g_free(cfg->source_port);
    g_free(cfg->source_dbname);
    g_free(cfg->source_password);
    g_free(cfg->source_username);
}

typedef struct { int fd; } WriteCtx;

static void get_current_time(char *buf, int buf_len)
{
    time_t now = time(NULL);
    struct tm *tm = localtime(&now);
    strftime(buf, buf_len, "%Y-%m-%d-%H-%M-%S", tm);
}

static const char* my_get_buffer_prefix(void) { return ""; }

static void* my_write_init(int tidnum, const char *task_name, int block_sock)
{
    WriteCtx *ctx = (WriteCtx *)calloc(1, sizeof(WriteCtx));
    if (!ctx) return NULL;

    char time_buf[24];
    get_current_time(time_buf, sizeof(time_buf));
    char filename[256];
    snprintf(filename, sizeof(filename), "sql_dump/%s-%d-%s.sql",
             task_name ?: "task", tidnum, time_buf);

    ctx->fd = open(filename, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (ctx->fd < 0)
    {
        char msg[256];
        snprintf(msg, sizeof(msg), "task_name:%s thread:%d open file failed\n",
                 task_name, tidnum);
        ReplyToClient(block_sock, msg);
    }
    return ctx;
}

static int my_write_exec(int tidnum, void *ctx, const char *buffer, long int size)
{
    (void)tidnum;
    WriteCtx *wc = (WriteCtx *)ctx;
    if (!wc || wc->fd < 0) return -1;
    return (write(wc->fd, buffer, size) < 0) ? -1 : 0;
}

static void my_write_fini(int tidnum, void *ctx)
{
    (void)tidnum;
    WriteCtx *wc = (WriteCtx *)ctx;
    if (!wc) return;
    if (wc->fd >= 0) close(wc->fd);
    free(wc);
}

static void* my_get_escape_conn(int tidnum, void *write_ctx)
{
    (void)tidnum;
    (void)write_ctx;
    return NULL;
}

static int my_gen_result(const PGresult *res, void *escape_conn,
                         char **result_point)
{
    assert(res != NULL);
    (void)escape_conn;
    int nfields = PQnfields(res);
    int ntuples = PQntuples(res);
    if (ntuples <= 0 || nfields <= 0) return 0;

    char *old_pos = *result_point;
    for (int row = 0; row < ntuples; row++)
    {
        cJSON *obj = cJSON_CreateObject();
        for (int col = 0; col < nfields; col++)
        {
            const char *cn = PQfname(res, col);
            if (PQgetisnull(res, row, col))
                cJSON_AddNullToObject(obj, cn);
            else
                cJSON_AddStringToObject(obj, cn, PQgetvalue(res, row, col));
        }
        char *json_all = cJSON_PrintUnformatted(obj);
        if (json_all)
        {
            FastStrcat2(result_point, json_all);
            **result_point = '\n';
            (*result_point)++;
            **result_point = '\0';
            free(json_all);
        }
        cJSON_Delete(obj);
    }
    return (int)(*result_point - old_pos);
}

/* ===================================================================
 *  入口函数
 * =================================================================== */
int pg_to_json(int acceptSockfd)
{
    MyTaskCfg cfg;
    memset(&cfg, 0, sizeof(cfg));

    const char *tmp;
    tmp = get_cfg("task_name");               cfg.task_name            = tmp ? g_strdup(tmp) : NULL;
    tmp = get_cfg("task_speed");              cfg.task_speed           = tmp ? g_strdup(tmp) : NULL;
    tmp = get_cfg("migrate_type");            cfg.migrate_type         = tmp ? g_strdup(tmp) : NULL;
    tmp = get_cfg("pump_sql");                cfg.pump_sql             = tmp ? g_strdup(tmp) : NULL;
    tmp = get_cfg("parallel_thread_per_task"); cfg.parallel_thread_per_task = tmp ? atoi(tmp) : 1;
    tmp = get_cfg("send_buffer_size");        cfg.send_buffer_size     = tmp ? atoi(tmp) : 1024;
    tmp = get_cfg("source_host");             cfg.source_host          = tmp ? g_strdup(tmp) : NULL;
    tmp = get_cfg("source_port");             cfg.source_port          = tmp ? g_strdup(tmp) : NULL;
    tmp = get_cfg("source_dbname");           cfg.source_dbname        = tmp ? g_strdup(tmp) : NULL;
    tmp = get_cfg("source_password");         cfg.source_password      = tmp ? g_strdup(tmp) : NULL;
    tmp = get_cfg("source_username");         cfg.source_username      = tmp ? g_strdup(tmp) : NULL;

    char source_conninfo[4096] = {0};
    snprintf(source_conninfo, sizeof(source_conninfo),
             "host=%s port=%s dbname=%s user=%s password=%s",
             cfg.source_host   ?: "", cfg.source_port   ?: "",
             cfg.source_dbname ?: "", cfg.source_username ?: "",
             cfg.source_password ?: "");

    PgEngineCallbacks cb;
    memset(&cb, 0, sizeof(cb));
    cb.gen_result          = my_gen_result;
    cb.get_escape_conn     = my_get_escape_conn;
    cb.write_init          = my_write_init;
    cb.write_exec          = my_write_exec;
    cb.write_fini          = my_write_fini;
    cb.get_buffer_prefix   = my_get_buffer_prefix;
    cb.page_size            = 16384;
    cb.speed_multiplier     = 1;
    cb.use_half_buffer_check = 1;

    int ret = pg_engine_run(acceptSockfd,
                            cfg.task_name, cfg.pump_sql,
                            cfg.parallel_thread_per_task, cfg.send_buffer_size,
                            source_conninfo, &cb);
    my_task_free(&cfg);
    return ret;
}