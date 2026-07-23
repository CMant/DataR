/* ===================================================================
 *  pg_to_doris.c — PG → Doris 迁移（基于 pg_engine）
 *
 *  将缓冲区中的 JSON 数据先写入 sql_dump 目录下的临时文件，
 *  然后调用 doris_stream_load.sh 脚本将文件导入到 Doris，
 *  脚本执行完毕后清空文件。
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
#include <time.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>

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
    char *dest_host;
    char *dest_port;
    char *dest_dbname;
    char *dest_table;
    char *dest_username;
    char *dest_password;
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
    g_free(cfg->dest_host);
    g_free(cfg->dest_port);
    g_free(cfg->dest_dbname);
    g_free(cfg->dest_table);
    g_free(cfg->dest_username);
    g_free(cfg->dest_password);
}

/* ===================================================================
 *  写入上下文 — 存储 Doris 连接参数 + 序列号 + 文件路径
 * =================================================================== */
typedef struct {
    char *dest_host;
    char *dest_port;
    char *dest_dbname;
    char *dest_table;
    char *dest_username;
    char *dest_password;
    int   seq;             /* 每线程序列号，用于生成唯一文件名 */
    int   tidnum;          /* 线程编号 */
    char  filepath[256];   /* 当前批次文件路径 */
} WriteCtx;

static void get_current_time(char *buf, int buf_len)
{
    time_t now = time(NULL);
    struct tm *tm = localtime(&now);
    strftime(buf, buf_len, "%Y-%m-%d-%H-%M-%S", tm);
}

static const char* my_get_buffer_prefix(void) { return ""; }

static void* my_write_init(int tidnum, const char *task_name, int block_sock)
{
    (void)task_name;
    (void)block_sock;
    WriteCtx *ctx = (WriteCtx *)calloc(1, sizeof(WriteCtx));
    if (!ctx) return NULL;

    const char *v;
    v = get_cfg("dest_host");     ctx->dest_host     = v ? g_strdup(v) : NULL;
    v = get_cfg("dest_port");     ctx->dest_port     = v ? g_strdup(v) : NULL;
    v = get_cfg("dest_dbname");   ctx->dest_dbname   = v ? g_strdup(v) : NULL;
    v = get_cfg("dest_table");    ctx->dest_table    = v ? g_strdup(v) : NULL;
    v = get_cfg("dest_username"); ctx->dest_username = v ? g_strdup(v) : NULL;
    v = get_cfg("dest_password"); ctx->dest_password = v ? g_strdup(v) : NULL;
    ctx->seq = 0;
    ctx->tidnum = tidnum;
    ctx->filepath[0] = '\0';

    /* 确保 sql_dump 目录存在 */
    mkdir("sql_dump", 0755);

    return ctx;
}

static int my_write_exec(int tidnum, void *ctx, const char *buffer, long int size)
{
    (void)tidnum;
    WriteCtx *wc = (WriteCtx *)ctx;
    if (!wc || size <= 0) return -1;

    /* 生成文件名：{tidnum}-{seq}-{timestamp}.sql */
    char time_buf[24];
    get_current_time(time_buf, sizeof(time_buf));
    snprintf(wc->filepath, sizeof(wc->filepath),
             "sql_dump/%d-%d-%s.sql",
             wc->tidnum, wc->seq++, time_buf);

    /* ========== 1. 将缓冲区内容写入文件 ========== */
    int fd = open(wc->filepath, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) return -1;

    ssize_t written_total = 0;
    while (written_total < size)
    {
        ssize_t n = write(fd, buffer + written_total, (size_t)(size - written_total));
        if (n <= 0)
        {
            close(fd);
            unlink(wc->filepath);
            return -1;
        }
        written_total += n;
    }
    close(fd);

    /* ========== 2. 调用 doris_stream_load.sh 脚本导入到 Doris ========== */
    char cmd[65536];
    int cmd_len = snprintf(cmd, sizeof(cmd),
        "bash doris_stream_load.sh '%s' '%s' '%s' '%s' '%s' '%s' '%s'",
        wc->filepath,
        wc->dest_host     ?: "",
        wc->dest_port     ?: "",
        wc->dest_dbname   ?: "",
        wc->dest_table    ?: "",
        wc->dest_username ?: "",
        wc->dest_password ?: "");
    if (cmd_len >= (int)sizeof(cmd))
    {
        unlink(wc->filepath);
        return -1;
    }

    FILE *fp = popen(cmd, "r");
    if (!fp)
    {
        unlink(wc->filepath);
        return -1;
    }

    /* 读取并丢弃脚本输出 */
    char discard[1024];
    while (fgets(discard, sizeof(discard), fp) != NULL);

    int status = pclose(fp);

    /* ========== 3. 清空文件（不管导入成功或失败，都清空） ========== */
    int clear_fd = open(wc->filepath, O_WRONLY | O_TRUNC);
    if (clear_fd >= 0) close(clear_fd);

    if (status != 0)
    {
        return -1;
    }

    return 0;
}

static void my_write_fini(int tidnum, void *ctx)
{
    (void)tidnum;
    WriteCtx *wc = (WriteCtx *)ctx;
    if (!wc) return;

    /* 如果还有未清理的文件，清理掉 */
    if (wc->filepath[0] != '\0')
    {
        unlink(wc->filepath);
    }

    g_free(wc->dest_host);
    g_free(wc->dest_port);
    g_free(wc->dest_dbname);
    g_free(wc->dest_table);
    g_free(wc->dest_username);
    g_free(wc->dest_password);
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
int pg_to_doris(int acceptSockfd)
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
    tmp = get_cfg("dest_host");               cfg.dest_host            = tmp ? g_strdup(tmp) : NULL;
    tmp = get_cfg("dest_port");               cfg.dest_port            = tmp ? g_strdup(tmp) : NULL;
    tmp = get_cfg("dest_dbname");             cfg.dest_dbname          = tmp ? g_strdup(tmp) : NULL;
    tmp = get_cfg("dest_table");              cfg.dest_table           = tmp ? g_strdup(tmp) : NULL;
    tmp = get_cfg("dest_username");           cfg.dest_username        = tmp ? g_strdup(tmp) : NULL;
    tmp = get_cfg("dest_password");           cfg.dest_password        = tmp ? g_strdup(tmp) : NULL;

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