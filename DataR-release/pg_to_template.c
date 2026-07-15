/* ===================================================================
 *  pg_to_template.c — 新增目标端的模板示例
 *
 *  本文件演示如何使用 pg_engine 快速实现一个新的 PG→目标 迁移。
 *  您只需实现 PgEngineCallbacks 中的 6 个回调和配置字段，
 *  然后自行填充 MyTaskCfg 结构体，再调用 pg_engine_run()。
 *
 *  MyTaskCfg 的字段从 get_cfg() 获取，您可以根据目标端需要自由
 *  增减字段。引擎只关心 task_name, pump_sql, parallel_thread_per_task,
 *  send_buffer_size, source_conninfo 这几个参数。
 *
 *  编译方式：
 *    gcc -fPIC -shared -o lib/libpg_to_template.so  \
 *        pg_to_template.c pg_engine.c               \
 *        -I./include `pkg-config --cflags --libs libpq glib-2.0` -lpthread
 *
 *  使用时，将 migrate_type 配置为 "pg_to_template"，read_from_db.c
 *  会自动加载 ./lib/libpg_to_template.so 并调用 pg_to_template()。
 * =================================================================== */

#include "pg_engine.h"
#include "include/common.h"
#include "include/network_service.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <fcntl.h>
#include <unistd.h>

/* ===================================================================
 *  本地任务配置（完全由本文件管理，引擎不关心）
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
    /* 目标端字段按需添加 */
    // char *dest_host;
    // char *dest_port;
    // char *dest_dbname;
    // char *dest_username;
    // char *dest_password;
    // char *dest_table;
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

/* ===================================================================
 *  写入上下文（每个线程独立一份）
 * =================================================================== */
typedef struct {
    int  fd;
    void *dest_conn;
} WriteCtx;

/* ===================================================================
 *  回调实现
 * =================================================================== */
static const char* my_get_buffer_prefix(void) { return ""; }

static void* my_write_init(int tidnum, const char *task_name, int block_sock)
{
    WriteCtx *ctx = (WriteCtx *)calloc(1, sizeof(WriteCtx));
    if (!ctx) return NULL;
    ctx->fd = -1;
    ctx->dest_conn = NULL;

    char filename[256];
    snprintf(filename, sizeof(filename), "sql_dump/%s-%d.txt",
             task_name ?: "task", tidnum);
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
    if (!wc) return -1;
    if (wc->fd >= 0 && write(wc->fd, buffer, size) < 0) return -1;
    return 0;
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
    (void)tidnum; (void)write_ctx; return NULL;
}

static int my_gen_result(const PGresult *res, void *escape_conn,
                         char **result_point)
{
    assert(res != NULL); (void)escape_conn;
    int nf = PQnfields(res), nt = PQntuples(res);
    if (nt <= 0 || nf <= 0) return 0;
    char *old = *result_point;
    for (int r = 0; r < nt; r++) {
        FastStrcat2(result_point, "(");
        for (int c = 0; c < nf; c++) {
            if (PQgetisnull(res, r, c)) FastStrcat2(result_point, "NULL,");
            else {
                char *val = PQgetvalue(res, r, c);
                FastStrcat2(result_point, "'");
                FastStrcat2(result_point, val);
                FastStrcat2(result_point, "',");
            }
        }
        *result_point = *result_point - 1;
        FastStrcat2(result_point, "),");
    }
    return (int)(*result_point - old);
}

/* ===================================================================
 *  入口函数
 * =================================================================== */
int pg_to_template(int acceptSockfd)
{
    MyTaskCfg cfg;
    memset(&cfg, 0, sizeof(cfg));

    cfg.task_name            = g_strdup(get_cfg("task_name") ?: "");
    cfg.task_speed           = g_strdup(get_cfg("task_speed") ?: "");
    cfg.migrate_type         = g_strdup(get_cfg("migrate_type") ?: "");
    cfg.pump_sql             = g_strdup(get_cfg("pump_sql") ?: "");
    cfg.parallel_thread_per_task = atoi(get_cfg("parallel_thread_per_task") ?: "1");
    cfg.send_buffer_size     = atoi(get_cfg("send_buffer_size") ?: "1024");
    cfg.source_host          = g_strdup(get_cfg("source_host") ?: "");
    cfg.source_port          = g_strdup(get_cfg("source_port") ?: "");
    cfg.source_dbname        = g_strdup(get_cfg("source_dbname") ?: "");
    cfg.source_password      = g_strdup(get_cfg("source_password") ?: "");
    cfg.source_username      = g_strdup(get_cfg("source_username") ?: "");

    char source_conninfo[4096] = {0};
    snprintf(source_conninfo, sizeof(source_conninfo),
             "host=%s port=%s dbname=%s user=%s password=%s",
             cfg.source_host, cfg.source_port, cfg.source_dbname,
             cfg.source_username, cfg.source_password);

    PgEngineCallbacks cb;
    memset(&cb, 0, sizeof(cb));
    cb.gen_result          = my_gen_result;
    cb.get_escape_conn     = my_get_escape_conn;
    cb.write_init          = my_write_init;
    cb.write_exec          = my_write_exec;
    cb.write_fini          = my_write_fini;
    cb.get_buffer_prefix   = my_get_buffer_prefix;
    cb.page_size            = 8192;
    cb.speed_multiplier     = 0;
    cb.use_half_buffer_check = 0;

    int ret = pg_engine_run(acceptSockfd,
                            cfg.task_name, cfg.pump_sql,
                            cfg.parallel_thread_per_task, cfg.send_buffer_size,
                            source_conninfo, &cb);
    my_task_free(&cfg);
    return ret;
}