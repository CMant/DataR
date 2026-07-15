/* ===================================================================
 *  pg_to_pg.c — PG → PG 迁移（基于 pg_engine）
 *
 *  TASK_INF 定义、创建、释放完全在此文件内，引擎不参与。
 * =================================================================== */

#include "include/pg_engine.h"
#include "include/common.h"
#include "include/network_service.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

/* ===================================================================
 *  TASK_INF 由调用方自行定义（引擎不关心）
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
    char *dest_username;
    char *dest_password;
    char *dest_host;
    char *dest_port;
    char *dest_dbname;
    char *dest_table;
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
    g_free(cfg->dest_username);
    g_free(cfg->dest_password);
    g_free(cfg->dest_host);
    g_free(cfg->dest_port);
    g_free(cfg->dest_dbname);
    g_free(cfg->dest_table);
}

/* ===================================================================
 *  写入上下文
 * =================================================================== */
typedef struct {
    PGconn *dest_conn;
} WriteCtx;

static const char* my_get_buffer_prefix(void)
{
    static char prefix[1024];
    const char *table = get_cfg("dest_table");
    snprintf(prefix, sizeof(prefix), "insert into %s values ", table ?: "unknown");
    return prefix;
}

static void* my_write_init(int tidnum, const char *task_name, int block_sock)
{
    WriteCtx *ctx = (WriteCtx *)calloc(1, sizeof(WriteCtx));
    if (!ctx) return NULL;

    const char *host     = get_cfg("dest_host") ?: "localhost";
    const char *port     = get_cfg("dest_port") ?: "5432";
    const char *dbname   = get_cfg("dest_dbname") ?: "";
    const char *user     = get_cfg("dest_username") ?: "";
    const char *password = get_cfg("dest_password") ?: "";

    char conninfo[4096];
    snprintf(conninfo, sizeof(conninfo),
             "host=%s port=%s dbname=%s user=%s password=%s",
             host, port, dbname, user, password);

    ctx->dest_conn = PQconnectdb(conninfo);
    char msg[256];
    if (PQstatus(ctx->dest_conn) != CONNECTION_BAD)
        snprintf(msg, sizeof(msg), "task_name:%s thread:%d connect_success\n",
                 task_name, tidnum);
    else
        snprintf(msg, sizeof(msg), "task_name:%s thread:%d connect_error: %s\n",
                 task_name, tidnum, PQerrorMessage(ctx->dest_conn));
    ReplyToClient(block_sock, msg);
    return ctx;
}

static int my_write_exec(int tidnum, void *ctx, const char *buffer, long int size)
{
    (void)tidnum;
    (void)size;
    WriteCtx *wc = (WriteCtx *)ctx;
    if (!wc || !wc->dest_conn) return -1;

    PGresult *res = PQexec(wc->dest_conn, buffer);
    ExecStatusType st = PQresultStatus(res);
    int ok = (st == PGRES_COMMAND_OK) ? 0 : -1;
    PQclear(res);
    return ok;
}

static void my_write_fini(int tidnum, void *ctx)
{
    (void)tidnum;
    WriteCtx *wc = (WriteCtx *)ctx;
    if (!wc) return;
    if (wc->dest_conn) PQfinish(wc->dest_conn);
    free(wc);
}

static void* my_get_escape_conn(int tidnum, void *write_ctx)
{
    (void)tidnum;
    (void)write_ctx;
    return (void *)pg_engine_source_conn;
}

static int my_gen_result(const PGresult *res, void *escape_conn,
                         char **result_point)
{
    assert(res != NULL);
    PGconn *source_conn = (PGconn *)escape_conn;

    int nfields = PQnfields(res);
    int ntuples = PQntuples(res);
    if (ntuples <= 0 || nfields <= 0) return 0;

    char *old_pos = *result_point;

    for (int row = 0; row < ntuples; row++)
    {
        FastStrcat2(result_point, "(");
        for (int col = 0; col < nfields; col++)
        {
            if (PQgetisnull(res, row, col))
                FastStrcat2(result_point, "NULL,");
            else
            {
                char *mx = PQgetvalue(res, row, col);
                int col_sz = PQgetlength(res, row, col);
                FastStrcat2(result_point, "'");
                int escaped = PQescapeStringConn(source_conn, *result_point,
                                                  mx, col_sz, NULL);
                *result_point += escaped;
                FastStrcat2(result_point, "',");
            }
        }
        *result_point = *result_point - 1;
        FastStrcat2(result_point, "),");
    }
    return (int)(*result_point - old_pos);
}

/* ===================================================================
 *  入口函数
 * =================================================================== */
int pg_to_pg(int acceptSockfd)
{
    MyTaskCfg cfg;
    memset(&cfg, 0, sizeof(cfg));

    const char *tmp;
    tmp = get_cfg("task_name");             cfg.task_name            = tmp ? g_strdup(tmp) : NULL;
    tmp = get_cfg("task_speed");            cfg.task_speed           = tmp ? g_strdup(tmp) : NULL;
    tmp = get_cfg("migrate_type");          cfg.migrate_type         = tmp ? g_strdup(tmp) : NULL;
    tmp = get_cfg("pump_sql");              cfg.pump_sql             = tmp ? g_strdup(tmp) : NULL;
    tmp = get_cfg("parallel_thread_per_task"); cfg.parallel_thread_per_task = tmp ? atoi(tmp) : 1;
    tmp = get_cfg("send_buffer_size");      cfg.send_buffer_size     = tmp ? atoi(tmp) : 1024;
    tmp = get_cfg("source_host");           cfg.source_host          = tmp ? g_strdup(tmp) : NULL;
    tmp = get_cfg("source_port");           cfg.source_port          = tmp ? g_strdup(tmp) : NULL;
    tmp = get_cfg("source_dbname");         cfg.source_dbname        = tmp ? g_strdup(tmp) : NULL;
    tmp = get_cfg("source_password");       cfg.source_password      = tmp ? g_strdup(tmp) : NULL;
    tmp = get_cfg("source_username");       cfg.source_username      = tmp ? g_strdup(tmp) : NULL;
    tmp = get_cfg("dest_username");         cfg.dest_username        = tmp ? g_strdup(tmp) : NULL;
    tmp = get_cfg("dest_password");         cfg.dest_password        = tmp ? g_strdup(tmp) : NULL;
    tmp = get_cfg("dest_host");             cfg.dest_host            = tmp ? g_strdup(tmp) : NULL;
    tmp = get_cfg("dest_port");             cfg.dest_port            = tmp ? g_strdup(tmp) : NULL;
    tmp = get_cfg("dest_dbname");           cfg.dest_dbname          = tmp ? g_strdup(tmp) : NULL;
    tmp = get_cfg("dest_table");            cfg.dest_table           = tmp ? g_strdup(tmp) : NULL;

    /* 源端连接串 */
    char source_conninfo[4096] = {0};
    snprintf(source_conninfo, sizeof(source_conninfo),
             "host=%s port=%s dbname=%s user=%s password=%s",
             cfg.source_host   ?: "",
             cfg.source_port   ?: "",
             cfg.source_dbname ?: "",
             cfg.source_username ?: "",
             cfg.source_password ?: "");

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
                            cfg.task_name,
                            cfg.pump_sql,
                            cfg.parallel_thread_per_task,
                            cfg.send_buffer_size,
                            source_conninfo,
                            &cb);

    my_task_free(&cfg);
    return ret;
}